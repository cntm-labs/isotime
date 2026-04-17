pub mod bloom;
pub mod compaction;
pub mod compressor;
pub mod memtable;
pub mod sstable;
pub mod wal;

use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTable;
use crate::storage::wal::{Wal, WalOp};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct StorageEngine {
    memtable: Arc<MemTable>,
    wal: Arc<Mutex<Wal>>,
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        let wal = Wal::new(wal_path)?;
        let entries = wal.recover()?;
        let memtable = MemTable::new();
        for entry in entries {
            match entry {
                WalOp::Put(key, value) => memtable.insert(key, value),
                WalOp::Delete(key) => memtable.delete(&key),
            }
        }

        Ok(Self {
            memtable: Arc::new(memtable),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| io::Error::other("WAL lock poisoned"))?;
        wal.append(&key, &value)?;
        self.memtable.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| io::Error::other("WAL lock poisoned"))?;
        wal.delete(key)?;
        self.memtable.delete(key);
        Ok(())
    }

    pub fn flush<P: AsRef<Path>>(&self, sstable_path: P) -> io::Result<()> {
        let snapshot = self.memtable.snapshot();
        SSTable::write(sstable_path.as_ref(), snapshot)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;

    #[test]
    fn test_storage_engine_put_get() {
        let wal_path = "test_engine.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let engine = StorageEngine::new(wal_path).unwrap();
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2"), None);

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_recovery() {
        let wal_path = "test_recovery.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        {
            let engine = StorageEngine::new(wal_path).unwrap();
            engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
            engine.delete(b"key1").unwrap();
        }

        {
            let engine = StorageEngine::new(wal_path).unwrap();
            assert_eq!(engine.get(b"key1"), None);
            assert_eq!(engine.get(b"key2"), Some(b"value2".to_vec()));
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_concurrent_put() {
        let wal_path = "test_concurrent.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let engine = Arc::new(StorageEngine::new(wal_path).unwrap());
        let num_threads = 4;
        let num_inserts = 1000;
        let mut handles = vec![];

        for i in 0..num_threads {
            let eng = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for j in 0..num_inserts {
                    let key = format!("thread-{}-key-{}", i, j).into_bytes();
                    let value = format!("value-{}", j).into_bytes();
                    eng.put(key, value).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..num_threads {
            for j in 0..num_inserts {
                let key = format!("thread-{}-key-{}", i, j).into_bytes();
                let expected_value = format!("value-{}", j).into_bytes();
                assert_eq!(engine.get(&key), Some(expected_value));
            }
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_flush() {
        let wal_path = "test_flush.wal";
        let sst_path = "test_flush.sst";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }
        if Path::new(sst_path).exists() {
            fs::remove_file(sst_path).unwrap();
        }

        let engine = StorageEngine::new(wal_path).unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.flush(sst_path).unwrap();

        let sstable = SSTable::open(Path::new(sst_path)).unwrap();
        assert_eq!(sstable.get(b"k1").unwrap(), Some(b"v1".to_vec()));

        fs::remove_file(wal_path).unwrap();
        fs::remove_file(sst_path).unwrap();
    }
}
