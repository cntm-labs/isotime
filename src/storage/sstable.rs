use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

#[path = "schema_generated.rs"]
mod schema_generated;
use schema_generated::isotime::storage as fbs;
use flatbuffers::FlatBufferBuilder;

pub struct SSTable {
    buffer: Vec<u8>,
}

impl SSTable {
    pub fn write(path: &Path, data: BTreeMap<Vec<u8>, Vec<u8>>) -> io::Result<()> {
        let mut fbb = FlatBufferBuilder::new();
        let mut entries = Vec::new();

        for (key, value) in data {
            let key_vec = fbb.create_vector(&key);
            let value_vec = fbb.create_vector(&value);
            let entry = fbs::Entry::create(&mut fbb, &fbs::EntryArgs {
                key: Some(key_vec),
                value: Some(value_vec),
            });
            entries.push(entry);
        }

        let entries_vec = fbb.create_vector(&entries);
        let sstable_data = fbs::SSTableData::create(&mut fbb, &fbs::SSTableDataArgs {
            entries: Some(entries_vec),
        });

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
        fbs::root_as_sstable_data(&buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Invalid FlatBuffers data: {:?}", e)))?;

        Ok(Self {
            buffer,
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<&[u8]>> {
        let sstable_data = fbs::root_as_sstable_data(&self.buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Invalid FlatBuffers data: {:?}", e)))?;

        if let Some(entries) = sstable_data.entries() {
            let mut low = 0;
            let mut high = entries.len();

            while low < high {
                let mid = low + (high - low) / 2;
                let entry = entries.get(mid);
                let entry_key = entry.key().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing key in entry"))?;
                
                match entry_key.bytes().cmp(key) {
                    std::cmp::Ordering::Equal => {
                        let value = entry.value().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing value in entry"))?;
                        return Ok(Some(value.bytes()));
                    }
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_sstable_write_read() {
        let path = PathBuf::from("test_sstable_write_read.db");
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
    fn test_sstable_persistence() {
        let path = PathBuf::from("test_sstable_persistence.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let mut data = BTreeMap::new();
        data.insert(b"a".to_vec(), b"1".to_vec());
        data.insert(b"b".to_vec(), b"2".to_vec());

        SSTable::write(&path, data).expect("Failed to write");

        let sstable = SSTable::open(&path).expect("Failed to open");
        assert_eq!(sstable.get(b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(sstable.get(b"b").unwrap(), Some(&b"2"[..]));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_sstable_empty() {
        let path = PathBuf::from("test_sstable_empty.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let data = BTreeMap::new();
        SSTable::write(&path, data).expect("Failed to write empty");

        let sstable = SSTable::open(&path).expect("Failed to open empty");
        assert_eq!(sstable.get(b"any").unwrap(), None);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_sstable_large_values() {
        let path = PathBuf::from("test_sstable_large.db");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let mut data = BTreeMap::new();
        let large_value = vec![0u8; 1024 * 1024]; // 1MB
        data.insert(b"large".to_vec(), large_value.clone());

        SSTable::write(&path, data).expect("Failed to write large");

        let sstable = SSTable::open(&path).expect("Failed to open large");
        assert_eq!(sstable.get(b"large").unwrap(), Some(&large_value[..]));

        fs::remove_file(&path).unwrap();
    }
}
