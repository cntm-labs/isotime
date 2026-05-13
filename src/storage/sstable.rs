use crate::storage::bloom::BloomFilter;
use crate::storage::cas::CASManager;
use crate::storage::compressor::{CompressionPolicy, CompressionType, Compressor};
use crate::storage::encryption::EncryptionManager;
use crate::storage::io_pool::IoPool;
use flatbuffers::FlatBufferBuilder;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;

#[path = "schema_generated.rs"]
#[allow(clippy::all)]
mod schema_generated;
use schema_generated::isotime::storage as fbs;

pub struct SSTable {
    buffer: Vec<u8>,
    bloom_filter: Option<BloomFilter>,
}

impl SSTable {
    #[allow(clippy::type_complexity)]
    pub async fn write(
        path: &Path,
        data: BTreeMap<Vec<u8>, (Vec<u8>, Vec<(u32, u64)>)>,
        tags: BTreeMap<String, Vec<Vec<u8>>>,
        enc: Option<&EncryptionManager>,
        policy: CompressionPolicy,
        cas: Option<&CASManager>,
        io_pool: &IoPool,
    ) -> io::Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        let mut entries = Vec::new();
        let mut value_store = HashMap::new();

        // Create Bloom Filter
        let mut bloom = BloomFilter::new(data.len().max(1), 0.01);

        for (i, (key, (value, clock))) in data.into_iter().enumerate() {
            bloom.add(&key);
            let key_vec = fbb.create_vector(&key);

            // Create Vector Clock
            let mut vc_entries = Vec::new();
            for (node_id, counter) in clock {
                vc_entries.push(fbs::VectorClockEntry::create(
                    &mut fbb,
                    &fbs::VectorClockEntryArgs { node_id, counter },
                ));
            }
            let vc_vec = fbb.create_vector(&vc_entries);
            let clock_offset = fbs::VectorClock::create(
                &mut fbb,
                &fbs::VectorClockArgs {
                    entries: Some(vc_vec),
                },
            );

            let (value_type, value_offset) =
                if policy != CompressionPolicy::Fastest && value_store.contains_key(&value) {
                    let &orig_idx = value_store.get(&value).unwrap();
                    // Value seen before, use RefValue
                    let ref_value =
                        fbs::RefValue::create(&mut fbb, &fbs::RefValueArgs { offset: orig_idx });
                    (fbs::ValueType::RefValue, ref_value.as_union_value())
                } else if policy == CompressionPolicy::ExtremeSpace && cas.is_some() {
                    // Check Global CAS
                    let cas_manager = cas.unwrap();
                    let hash = cas_manager.put(&value).await?;
                    let hash_vec = fbb.create_vector(&hash);
                    let hash_value = fbs::HashValue::create(
                        &mut fbb,
                        &fbs::HashValueArgs {
                            hash: Some(hash_vec),
                        },
                    );
                    (fbs::ValueType::HashValue, hash_value.as_union_value())
                } else {
                    // New value (or Fastest policy which skips de-dupe), use RawValue
                    if policy != CompressionPolicy::Fastest {
                        value_store.insert(value.clone(), i as u32);
                    }

                    // Compress value
                    let (ctype, compressed_data) = Compressor::compress(&value, policy);
                    let fbs_ctype = match ctype {
                        CompressionType::None => fbs::CompressionType::None,
                        CompressionType::DeltaDelta => fbs::CompressionType::DeltaDelta,
                        CompressionType::BitPackedDelta => fbs::CompressionType::BitPackedDelta,
                    };

                    let data_vec = fbb.create_vector(&compressed_data);
                    let raw_value = fbs::RawValue::create(
                        &mut fbb,
                        &fbs::RawValueArgs {
                            data: Some(data_vec),
                            compression: fbs_ctype,
                        },
                    );
                    (fbs::ValueType::RawValue, raw_value.as_union_value())
                };

            let entry = fbs::Entry::create(
                &mut fbb,
                &fbs::EntryArgs {
                    key: Some(key_vec),
                    value_type,
                    value: Some(value_offset),
                    clock: Some(clock_offset),
                },
            );
            entries.push(entry);
        }

        let mut tag_indexes = Vec::new();
        for (tag_name, keys) in tags {
            let tag_str = fbb.create_string(&tag_name);
            let mut keys_offsets = Vec::new();
            for k in keys {
                let k_vec = fbb.create_vector(&k);
                keys_offsets.push(fbs::TagKey::create(
                    &mut fbb,
                    &fbs::TagKeyArgs { key: Some(k_vec) },
                ));
            }
            let keys_vec = fbb.create_vector(&keys_offsets);
            tag_indexes.push(fbs::TagIndex::create(
                &mut fbb,
                &fbs::TagIndexArgs {
                    tag: Some(tag_str),
                    keys: Some(keys_vec),
                },
            ));
        }
        let tag_indexes_vec = fbb.create_vector(&tag_indexes);

        let entries_vec = fbb.create_vector(&entries);
        let bloom_bytes = bloom.to_bytes();
        let bloom_vec = fbb.create_vector(&bloom_bytes);

        let sstable_data = fbs::SSTableData::create(
            &mut fbb,
            &fbs::SSTableDataArgs {
                entries: Some(entries_vec),
                bloom_filter: Some(bloom_vec),
                num_hashes: bloom.num_hashes() as u32,
                tag_indexes: Some(tag_indexes_vec),
            },
        );

        fbb.finish(sstable_data, None);
        let mut final_buffer = fbb.finished_data().to_vec();

        if let Some(e) = enc {
            final_buffer = e.encrypt(&final_buffer)?;
        }

        // Use io_pool for kernel-level async write
        io_pool.write(path, 0, final_buffer).await?;

        Ok(())
    }

    pub async fn open(
        path: &Path,
        enc: Option<&EncryptionManager>,
        io_pool: &IoPool,
    ) -> io::Result<Self> {
        // We need to know the size to read via io_uring.
        // In a production engine we'd have a footer or fixed size header.
        // For now we use standard metadata to get size then read_at.
        let size = std::fs::metadata(path)?.len();
        let mut buffer = io_pool.read(path, 0, size as usize).await?;

        if let Some(e) = enc {
            buffer = e.decrypt(&buffer)?;
        }

        let bloom_filter = {
            let data = fbs::root_as_sstable_data(&buffer).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("FlatBuffer error: {}", e),
                )
            })?;
            data.bloom_filter()
                .map(|b| BloomFilter::from_vec(b.bytes().to_vec(), data.num_hashes() as usize))
        };

        Ok(Self {
            buffer,
            bloom_filter,
        })
    }

    pub async fn get(&self, key: &[u8], cas: Option<&CASManager>) -> io::Result<Option<Vec<u8>>> {
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.contains(key) {
                return Ok(None);
            }
        }

        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        // Binary search
        let mut low = 0;
        let mut high = entries.len();

        while low < high {
            let mid = low + (high - low) / 2;
            let entry = entries.get(mid);
            let entry_key = entry
                .key()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Entry missing key"))?;

            match key.cmp(entry_key.bytes()) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(self.resolve_value(entry, mid, cas).await?));
                }
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
            }
        }

        Ok(None)
    }

    pub async fn get_with_clock(
        &self,
        key: &[u8],
        cas: Option<&CASManager>,
    ) -> io::Result<Option<(Vec<u8>, Vec<(u32, u64)>)>> {
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.contains(key) {
                return Ok(None);
            }
        }

        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        // Binary search
        let mut low = 0;
        let mut high = entries.len();

        while low < high {
            let mid = low + (high - low) / 2;
            let entry = entries.get(mid);
            let entry_key = entry
                .key()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Entry missing key"))?;

            match key.cmp(entry_key.bytes()) {
                std::cmp::Ordering::Equal => {
                    let val = self.resolve_value(entry, mid, cas).await?;
                    let mut clock = Vec::new();
                    if let Some(vc) = entry.clock() {
                        if let Some(vc_entries) = vc.entries() {
                            for vc_entry in vc_entries {
                                clock.push((vc_entry.node_id(), vc_entry.counter()));
                            }
                        }
                    }
                    return Ok(Some((val, clock)));
                }
                std::cmp::Ordering::Less => high = mid,
                std::cmp::Ordering::Greater => low = mid + 1,
            }
        }

        Ok(None)
    }

    async fn resolve_value(
        &self,
        entry: fbs::Entry<'_>,
        _current_idx: usize,
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<u8>> {
        match entry.value_type() {
            fbs::ValueType::RawValue => {
                let raw = entry.value_as_raw_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Entry missing raw value")
                })?;
                let compressed_data = raw.data().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "RawValue missing data")
                })?;
                let ctype = match raw.compression() {
                    fbs::CompressionType::None => CompressionType::None,
                    fbs::CompressionType::DeltaDelta => CompressionType::DeltaDelta,
                    fbs::CompressionType::BitPackedDelta => CompressionType::BitPackedDelta,
                    _ => CompressionType::None,
                };
                Ok(Compressor::decompress(ctype, compressed_data.bytes()))
            }
            fbs::ValueType::RefValue => {
                let ref_val = entry.value_as_ref_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Entry missing ref value")
                })?;
                let data = fbs::root_as_sstable_data(&self.buffer).unwrap();
                let entries = data.entries().unwrap();
                let orig_entry = entries.get(ref_val.offset() as usize);
                // Recursive call to resolve (guaranteed to be RawValue by write logic)
                Box::pin(self.resolve_value(orig_entry, ref_val.offset() as usize, cas)).await
            }
            fbs::ValueType::HashValue => {
                let hash_val = entry.value_as_hash_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Entry missing hash value")
                })?;
                let hash_bytes = hash_val.hash().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "HashValue missing hash")
                })?;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(hash_bytes.bytes());
                let cas_manager = cas
                    .ok_or_else(|| io::Error::other("Global CAS manager required for HashValue"))?;

                cas_manager.get(&hash).await.map(|v| v.unwrap_or_default())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown value type",
            )),
        }
    }

    pub async fn get_by_tag(&self, tag: &str) -> io::Result<Vec<Vec<u8>>> {
        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        if let Some(tag_indexes) = data.tag_indexes() {
            for idx in tag_indexes {
                if idx.tag() == Some(tag) {
                    if let Some(keys) = idx.keys() {
                        let mut result = Vec::new();
                        for k in keys {
                            if let Some(key_data) = k.key() {
                                result.push(key_data.bytes().to_vec());
                            }
                        }
                        return Ok(result);
                    }
                }
            }
        }
        Ok(Vec::new())
    }

    pub async fn get_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        let mut results = Vec::new();

        // Find starting point via binary search
        let mut low = 0;
        let mut high = entries.len();
        let mut start_idx = entries.len();

        while low < high {
            let mid = low + (high - low) / 2;
            let entry = entries.get(mid);
            let entry_key = entry.key().unwrap();

            if entry_key.bytes() >= start_key {
                start_idx = mid;
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        // Scan from start_idx
        for i in start_idx..entries.len() {
            let entry = entries.get(i);
            let entry_key = entry.key().unwrap();

            if entry_key.bytes() >= end_key {
                break;
            }

            let val = self.resolve_value(entry, i, cas).await?;
            results.push((entry_key.bytes().to_vec(), val));
        }

        Ok(results)
    }

    #[allow(clippy::type_complexity)]
    pub async fn get_range_with_clock(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<(Vec<u8>, (Vec<u8>, Vec<(u32, u64)>))>> {
        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        let mut results = Vec::new();

        // Find starting point
        let mut low = 0;
        let mut high = entries.len();
        let mut start_idx = entries.len();

        while low < high {
            let mid = low + (high - low) / 2;
            let entry = entries.get(mid);
            let entry_key = entry.key().unwrap();

            if entry_key.bytes() >= start_key {
                start_idx = mid;
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        for i in start_idx..entries.len() {
            let entry = entries.get(i);
            let entry_key = entry.key().unwrap();

            if entry_key.bytes() >= end_key {
                break;
            }

            let val = self.resolve_value(entry, i, cas).await?;
            let mut clock = Vec::new();
            if let Some(vc) = entry.clock() {
                if let Some(vc_entries) = vc.entries() {
                    for vc_entry in vc_entries {
                        clock.push((vc_entry.node_id(), vc_entry.counter()));
                    }
                }
            }
            results.push((entry_key.bytes().to_vec(), (val, clock)));
        }

        Ok(results)
    }

    pub fn get_cas_references(&self) -> io::Result<Vec<[u8; 32]>> {
        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        let mut refs = Vec::new();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            if entry.value_type() == fbs::ValueType::HashValue {
                let hash_val = entry.value_as_hash_value().unwrap();
                let hash_bytes = hash_val.hash().unwrap();
                let mut hash = [0u8; 32];
                hash.copy_from_slice(hash_bytes.bytes());
                refs.push(hash);
            }
        }

        Ok(refs)
    }

    pub async fn all_entries(
        &self,
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<(Vec<u8>, (Vec<u8>, Vec<(u32, u64)>))>> {
        let data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("FlatBuffer error: {}", e),
            )
        })?;

        let entries = data
            .entries()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SSTable missing entries"))?;

        let mut results = Vec::new();
        for i in 0..entries.len() {
            let entry = entries.get(i);
            let entry_key = entry.key().unwrap();
            let val = self.resolve_value(entry, i, cas).await?;
            let mut clock = Vec::new();
            if let Some(vc) = entry.clock() {
                if let Some(vc_entries) = vc.entries() {
                    for vc_entry in vc_entries {
                        clock.push((vc_entry.node_id(), vc_entry.counter()));
                    }
                }
            }
            results.push((entry_key.bytes().to_vec(), (val, clock)));
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sstable_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let io_pool = IoPool::new(1);

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), (b"value1".to_vec(), vec![(1, 10)]));
        data.insert(
            b"key2".to_vec(),
            (b"value2".to_vec(), vec![(1, 11), (2, 5)]),
        );

        SSTable::write(
            &path,
            data,
            BTreeMap::new(),
            None,
            CompressionPolicy::Balanced,
            None,
            &io_pool,
        )
        .await
        .expect("Failed to write SSTable");

        let sstable = SSTable::open(&path, None, &io_pool)
            .await
            .expect("Failed to open SSTable");

        let (val1, clock1) = sstable
            .get_with_clock(b"key1", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(val1, b"value1");
        assert_eq!(clock1, vec![(1, 10)]);

        let (val2, clock2) = sstable
            .get_with_clock(b"key2", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(val2, b"value2");
        assert_eq!(clock2, vec![(1, 11), (2, 5)]);

        assert!(sstable.get(b"key3", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_sstable_tag_indexing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_tags.sst");
        let io_pool = IoPool::new(1);

        let mut data = BTreeMap::new();
        data.insert(b"k1".to_vec(), (b"v1".to_vec(), vec![]));
        data.insert(b"k2".to_vec(), (b"v2".to_vec(), vec![]));

        let mut tags = BTreeMap::new();
        tags.insert("tag1".to_string(), vec![b"k1".to_vec(), b"k2".to_vec()]);

        SSTable::write(
            &path,
            data,
            tags,
            None,
            CompressionPolicy::Balanced,
            None,
            &io_pool,
        )
        .await
        .expect("Failed to write SSTable");

        let sstable = SSTable::open(&path, None, &io_pool)
            .await
            .expect("Failed to open SSTable");
        let keys = sstable.get_by_tag("tag1").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"k1".to_vec()));
    }
}
