use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

#[path = "schema_generated.rs"]
mod schema_generated;
use crate::storage::bloom::BloomFilter;
use flatbuffers::FlatBufferBuilder;
use schema_generated::isotime::storage as fbs;

pub struct SSTable {
    buffer: Vec<u8>,
    bloom_filter: Option<BloomFilter>,
}

impl SSTable {
    pub fn write(path: &Path, data: BTreeMap<Vec<u8>, Vec<u8>>) -> io::Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        let mut entries = Vec::new();

        // Create Bloom Filter
        let mut bloom = BloomFilter::new(data.len().max(1), 0.01);

        for (key, value) in data {
            bloom.add(&key);
            let key_vec = fbb.create_vector(&key);
            let value_vec = fbb.create_vector(&value);
            let entry = fbs::Entry::create(
                &mut fbb,
                &fbs::EntryArgs {
                    key: Some(key_vec),
                    value: Some(value_vec),
                },
            );
            entries.push(entry);
        }

        let bloom_bytes = bloom.to_bytes();
        let bloom_vec = fbb.create_vector(&bloom_bytes);

        let entries_vec = fbb.create_vector(&entries);
        let sstable_data = fbs::SSTableData::create(
            &mut fbb,
            &fbs::SSTableDataArgs {
                entries: Some(entries_vec),
                bloom_filter: Some(bloom_vec),
                num_hashes: bloom.num_hashes() as u32,
            },
        );

        fbb.finish(sstable_data, None);
        let finished_data = fbb.finished_data();

        let mut file = File::create(path)?;
        file.write_all(finished_data)?;
        Ok(())
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Basic verification
        let sstable_data = fbs::root_as_sstable_data(&buffer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid FlatBuffers data: {:?}", e),
            )
        })?;

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

    pub fn get(&self, key: &[u8]) -> io::Result<Option<&[u8]>> {
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
                        let value = entry.value().ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "Missing value in entry")
                        })?;
                        return Ok(Some(value.bytes()));
                    }
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                }
            }
        }
        Ok(None)
    }

    pub fn all_entries(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
                let value = entry
                    .value()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing value"))?
                    .bytes()
                    .to_vec();
                result.push((key, value));
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_sstable_write_read_with_bloom() {
        let path = PathBuf::from("test_sstable_bloom.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), b"value1".to_vec());
        data.insert(b"key2".to_vec(), b"value2".to_vec());

        SSTable::write(&path, data).expect("Failed to write SSTable");

        let sstable = SSTable::open(&path).expect("Failed to open SSTable");

        // Positive cases
        assert_eq!(sstable.get(b"key1").unwrap(), Some(&b"value1"[..]));
        assert_eq!(sstable.get(b"key2").unwrap(), Some(&b"value2"[..]));

        // Negative case (Bloom filter should prune this)
        assert_eq!(sstable.get(b"non_existent").unwrap(), None);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_sstable_write_read() {
        let path = PathBuf::from("test_sstable_write_read_v2.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let mut data = BTreeMap::new();
        data.insert(b"key1".to_vec(), b"value1".to_vec());
        data.insert(b"key2".to_vec(), b"value2".to_vec());
        data.insert(b"key3".to_vec(), b"value3".to_vec());

        SSTable::write(&path, data).expect("Failed to write SSTable");

        let sstable = SSTable::open(&path).expect("Failed to open SSTable");
        assert_eq!(
            sstable.get(b"key1").expect("Failed to get key1"),
            Some(&b"value1"[..])
        );
        assert_eq!(
            sstable.get(b"key2").expect("Failed to get key2"),
            Some(&b"value2"[..])
        );
        assert_eq!(
            sstable.get(b"key3").expect("Failed to get key3"),
            Some(&b"value3"[..])
        );
        assert_eq!(sstable.get(b"key4").expect("Failed to get key4"), None);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_sstable_all_entries() {
        let path = PathBuf::from("test_sstable_all_entries.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let mut data = BTreeMap::new();
        data.insert(b"a".to_vec(), b"1".to_vec());
        data.insert(b"b".to_vec(), b"2".to_vec());

        SSTable::write(&path, data).unwrap();

        let sstable = SSTable::open(&path).unwrap();
        let entries = sstable.all_entries().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));

        fs::remove_file(&path).unwrap();
    }
}
