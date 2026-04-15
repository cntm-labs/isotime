use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub struct SSTable {
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>, // Key to offset in file
}

#[allow(dead_code)]
impl SSTable {
    pub fn write(path: &Path, data: BTreeMap<Vec<u8>, Vec<u8>>) -> io::Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        for (key, value) in data {
            // Write key length
            writer.write_all(&(key.len() as u64).to_be_bytes())?;
            // Write key
            writer.write_all(&key)?;
            // Write value length
            writer.write_all(&(value.len() as u64).to_be_bytes())?;
            // Write value
            writer.write_all(&value)?;
        }

        writer.flush()?;
        Ok(())
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut current_offset = 0;

        loop {
            let mut key_len_buf = [0u8; 8];
            if let Err(e) = reader.read_exact(&mut key_len_buf) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break; // EOF
                }
                return Err(e);
            }
            let key_len = u64::from_be_bytes(key_len_buf);
            let mut key = vec![0u8; key_len as usize];
            reader.read_exact(&mut key)?;

            index.insert(key, current_offset);

            let mut value_len_buf = [0u8; 8];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u64::from_be_bytes(value_len_buf);

            // Skip value for now to populate index quickly
            reader.seek(SeekFrom::Current(value_len as i64))?;

            current_offset += 8 + key_len + 8 + value_len;
        }

        Ok(Self {
            path: path.to_path_buf(),
            index,
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(&offset) = self.index.get(key) {
            let mut file = File::open(&self.path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::new(file);

            let mut key_len_buf = [0u8; 8];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u64::from_be_bytes(key_len_buf);

            // Skip key
            reader.seek(SeekFrom::Current(key_len as i64))?;

            let mut value_len_buf = [0u8; 8];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u64::from_be_bytes(value_len_buf);
            let mut value = vec![0u8; value_len as usize];
            reader.read_exact(&mut value)?;

            return Ok(Some(value));
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

        SSTable::write(&path, data.clone()).expect("Failed to write SSTable");

        let sstable = SSTable::open(&path).expect("Failed to open SSTable");
        assert_eq!(
            sstable.get(b"key1").expect("Failed to get key1"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            sstable.get(b"key2").expect("Failed to get key2"),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            sstable.get(b"key3").expect("Failed to get key3"),
            Some(b"value3".to_vec())
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
        assert_eq!(sstable.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(sstable.get(b"b").unwrap(), Some(b"2".to_vec()));

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
        assert_eq!(sstable.get(b"large").unwrap(), Some(large_value));

        fs::remove_file(&path).unwrap();
    }
}
