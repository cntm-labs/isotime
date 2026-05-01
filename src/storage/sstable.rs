use crate::storage::bloom::BloomFilter;
use crate::storage::cas::CASManager;
use crate::storage::compressor::{CompressionPolicy, CompressionType, Compressor};
use crate::storage::encryption::EncryptionManager;
use flatbuffers::FlatBufferBuilder;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "schema_generated.rs"]
#[allow(clippy::all)]
mod schema_generated;
use schema_generated::isotime::storage as fbs;

pub struct SSTable {
    buffer: Vec<u8>,
    bloom_filter: Option<BloomFilter>,
}

impl SSTable {
    pub async fn write(
        path: &Path,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        tags: BTreeMap<String, Vec<Vec<u8>>>,
        enc: Option<&EncryptionManager>,
        policy: CompressionPolicy,
        cas: Option<&CASManager>,
    ) -> io::Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        let mut entries = Vec::new();
        let mut value_store = HashMap::new();

        // Create Bloom Filter
        let mut bloom = BloomFilter::new(data.len().max(1), 0.01);

        for (i, (key, value)) in data.into_iter().enumerate() {
            bloom.add(&key);
            let key_vec = fbb.create_vector(&key);

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
                },
            );
            entries.push(entry);
        }

        let mut tag_indexes = Vec::new();
        for (tag_name, keys) in tags {
            let tag_str = fbb.create_string(&tag_name);
            let mut keys_offsets = Vec::new();
            for key in keys {
                let k_vec = fbb.create_vector(&key);
                keys_offsets.push(fbs::TagKey::create(&mut fbb, &fbs::TagKeyArgs { key: Some(k_vec) }));
            }
            let keys_vec = fbb.create_vector(&keys_offsets);
            let tag_index = fbs::TagIndex::create(
                &mut fbb,
                &fbs::TagIndexArgs {
                    tag: Some(tag_str),
                    keys: Some(keys_vec),
                },
            );
            tag_indexes.push(tag_index);
        }
        let tag_indexes_vec = fbb.create_vector(&tag_indexes);

        let bloom_bytes = bloom.to_bytes();
        let bloom_vec = fbb.create_vector(&bloom_bytes);

        let entries_vec = fbb.create_vector(&entries);
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
        let finished_data = fbb.finished_data();

        let data_to_write = if let Some(manager) = enc {
            manager.encrypt(finished_data)?
        } else {
            finished_data.to_vec()
        };

        let mut file = fs::File::create(path).await?;
        file.write_all(&data_to_write).await?;
        file.sync_all().await?;
        Ok(())
    }

    pub async fn open(path: &Path, enc: Option<&EncryptionManager>) -> io::Result<Self> {
        let mut file = fs::File::open(path).await?;
        let mut raw_buffer = Vec::new();
        file.read_to_end(&mut raw_buffer).await?;

        let buffer = if let Some(manager) = enc {
            manager.decrypt(&raw_buffer)?
        } else {
            raw_buffer
        };

        // Basic verification
        let _ = fbs::root_as_sstable_data(&buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid FlatBuffers data (decryption failed or wrong key?): {:?}",
                    e
                ),
            )
        })?;

        let sstable_data = fbs::root_as_sstable_data(&buffer).unwrap();

        let bloom_filter = if let (Some(bloom_bytes), num_hashes) =
            (sstable_data.bloom_filter(), sstable_data.num_hashes())
        {
            Some(BloomFilter::from_vec(
                bloom_bytes.bytes().to_vec(),
                num_hashes as usize,
            ))
        } else {
            None
        };

        Ok(Self {
            buffer,
            bloom_filter,
        })
    }

    async fn resolve_value<'a>(
        &'a self,
        entry: fbs::Entry<'a>,
        entries: flatbuffers::Vector<'a, ::flatbuffers::ForwardsUOffset<fbs::Entry<'a>>>,
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<u8>> {
        match entry.value_type() {
            fbs::ValueType::RawValue => {
                let raw = entry.value_as_raw_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Missing RawValue")
                })?;
                let data = raw
                    .data()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Missing data in RawValue")
                    })?
                    .bytes();

                let ctype = match raw.compression() {
                    fbs::CompressionType::None => CompressionType::None,
                    fbs::CompressionType::DeltaDelta => CompressionType::DeltaDelta,
                    fbs::CompressionType::BitPackedDelta => CompressionType::BitPackedDelta,
                    _ => CompressionType::None,
                };

                Ok(Compressor::decompress(ctype, data))
            }
            fbs::ValueType::RefValue => {
                let ref_val = entry.value_as_ref_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Missing RefValue")
                })?;
                let orig_idx = ref_val.offset() as usize;
                if orig_idx >= entries.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid RefValue offset",
                    ));
                }
                let orig_entry = entries.get(orig_idx);
                // Recursion in async needs to be boxed
                Box::pin(self.resolve_value(orig_entry, entries, cas)).await
            }
            fbs::ValueType::HashValue => {
                let hash_val = entry.value_as_hash_value().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Missing HashValue")
                })?;
                let hash_bytes = hash_val
                    .hash()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Missing hash in HashValue")
                    })?
                    .bytes();

                let mut hash = [0u8; 32];
                if hash_bytes.len() != 32 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid hash length",
                    ));
                }
                hash.copy_from_slice(hash_bytes);

                if let Some(cas_manager) = cas {
                    cas_manager.get(&hash).await?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "Value not found in CAS")
                    })
                } else {
                    Err(io::Error::other("CASManager not provided for HashValue"))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown ValueType",
            )),
        }
    }

    pub async fn get(&self, key: &[u8], cas: Option<&CASManager>) -> io::Result<Option<Vec<u8>>> {
        // Check Bloom Filter first
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.contains(key) {
                return Ok(None);
            }
        }

        let sstable_data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid FlatBuffers data: {:?}", e),
            )
        })?;

        if let Some(entries) = sstable_data.entries() {
            let mut low = 0;
            let mut high = entries.len();

            while low < high {
                let mid = low + (high - low) / 2;
                let entry = entries.get(mid);
                let entry_key = entry.key().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Missing key in entry")
                })?;

                match entry_key.bytes().cmp(key) {
                    std::cmp::Ordering::Equal => {
                        let val_bytes = self.resolve_value(entry, entries, cas).await?;
                        return Ok(Some(val_bytes));
                    }
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                }
            }
        }
        Ok(None)
    }

    pub async fn get_by_tag(&self, tag: &str) -> io::Result<Vec<Vec<u8>>> {
        let sstable_data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid FlatBuffers data: {:?}", e),
            )
        })?;

        let mut result = Vec::new();
        if let Some(tag_indexes) = sstable_data.tag_indexes() {
            for i in 0..tag_indexes.len() {
                let tag_idx = tag_indexes.get(i);
                if tag_idx.tag().unwrap() == tag {
                    if let Some(keys) = tag_idx.keys() {
                        for j in 0..keys.len() {
                            result.push(keys.get(j).key().unwrap().bytes().to_vec());
                        }
                    }
                    break;
                }
            }
        }
        Ok(result)
    }

    pub async fn all_entries(&self, cas: Option<&CASManager>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let sstable_data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid FlatBuffers data: {:?}", e),
            )
        })?;

        let mut result = Vec::new();
        if let Some(entries) = sstable_data.entries() {
            for i in 0..entries.len() {
                let entry = entries.get(i);
                let key = entry
                    .key()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing key"))?
                    .bytes()
                    .to_vec();
                let value = self.resolve_value(entry, entries, cas).await?;
                result.push((key, value));
            }
        }
        Ok(result)
    }

    pub async fn get_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        cas: Option<&CASManager>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let sstable_data = fbs::root_as_sstable_data(&self.buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid FlatBuffers data: {:?}", e),
            )
        })?;

        let mut result = Vec::new();
        if let Some(entries) = sstable_data.entries() {
            let mut low = 0;
            let mut high = entries.len();

            // Find first element >= start_key
            while low < high {
                let mid = low + (high - low) / 2;
                let entry = entries.get(mid);
                let entry_key = entry.key().unwrap().bytes();
                if entry_key < start_key {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            // Iterate until key >= end_key
            for i in low..entries.len() {
                let entry = entries.get(i);
                let key = entry.key().unwrap().bytes();
                if key >= end_key {
                    break;
                }

                let val_bytes = self.resolve_value(entry, entries, cas).await?;
                result.push((key.to_vec(), val_bytes));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs as std_fs;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_sstable_write_read_with_bloom() {
        let path = PathBuf::from("test_sstable_bloom.db");
        if path.exists() {
            let _ = std_fs::remove_file(&path);
        }

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), b"value1".to_vec());
        data.insert(b"key2".to_vec(), b"value2".to_vec());

        SSTable::write(&path, data, BTreeMap::new(), None, CompressionPolicy::Balanced, None)
            .await
            .expect("Failed to write SSTable");

        let sstable = SSTable::open(&path, None).await.expect("Failed to open SSTable");

        // Positive cases
        assert_eq!(
            sstable.get(b"key1", None).await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            sstable.get(b"key2", None).await.unwrap(),
            Some(b"value2".to_vec())
        );

        // Negative case (Bloom filter should prune this)
        assert_eq!(sstable.get(b"non_existent", None).await.unwrap(), None);

        let _ = std_fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_sstable_value_sharing() {
        let path = PathBuf::from("test_value_sharing.db");
        if path.exists() {
            let _ = std_fs::remove_file(&path);
        }

        let mut data = BTreeMap::new();
        let common_value = b"this is a very long value that should be shared".to_vec();
        for i in 0..100 {
            data.insert(format!("key{:03}", i).into_bytes(), common_value.clone());
        }

        SSTable::write(&path, data, BTreeMap::new(), None, CompressionPolicy::Balanced, None)
            .await
            .expect("Failed to write SSTable");

        let file_size = std_fs::metadata(&path).unwrap().len();
        assert!(file_size < 4500, "File size too large: {}", file_size);

        let sstable = SSTable::open(&path, None).await.expect("Failed to open SSTable");
        for i in 0..100 {
            assert_eq!(
                sstable
                    .get(format!("key{:03}", i).as_bytes(), None)
                    .await
                    .unwrap(),
                Some(common_value.clone())
            );
        }

        let _ = std_fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_sstable_write_read() {
        let path = PathBuf::from("test_sstable_write_read_v2.db");
        if path.exists() {
            let _ = std_fs::remove_file(&path);
        }

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), b"value1".to_vec());
        data.insert(b"key2".to_vec(), b"value2".to_vec());
        data.insert(b"key3".to_vec(), b"value3".to_vec());

        SSTable::write(&path, data, BTreeMap::new(), None, CompressionPolicy::Balanced, None)
            .await
            .expect("Failed to write SSTable");

        let sstable = SSTable::open(&path, None).await.expect("Failed to open SSTable");
        assert_eq!(
            sstable.get(b"key1", None).await.expect("Failed to get key1"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            sstable.get(b"key2", None).await.expect("Failed to get key2"),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            sstable.get(b"key3", None).await.expect("Failed to get key3"),
            Some(b"value3".to_vec())
        );
        assert_eq!(
            sstable.get(b"key4", None).await.expect("Failed to get key4"),
            None
        );

        let _ = std_fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_sstable_tag_indexing() {
        let path = PathBuf::from("test_sstable_tags.db");
        if path.exists() {
            let _ = std_fs::remove_file(&path);
        }

        let mut data = BTreeMap::new();
        data.insert(b"k1".to_vec(), b"v1".to_vec());
        data.insert(b"k2".to_vec(), b"v2".to_vec());

        let mut tags = BTreeMap::new();
        tags.insert("tag1".to_string(), vec![b"k1".to_vec(), b"k2".to_vec()]);

        SSTable::write(&path, data, tags, None, CompressionPolicy::Balanced, None)
            .await
            .expect("Failed to write SSTable");

        let sstable = SSTable::open(&path, None).await.expect("Failed to open SSTable");
        let keys = sstable.get_by_tag("tag1").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"k1".to_vec()));
        assert!(keys.contains(&b"k2".to_vec()));

        let _ = std_fs::remove_file(&path);
    }
}
