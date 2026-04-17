use crossbeam_skiplist::SkipMap;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct MemTable {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn delete(&self, key: &[u8]) {
        self.map.remove(key);
    }

    pub fn snapshot(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = BTreeMap::new();
        for entry in self.map.iter() {
            map.insert(entry.key().clone(), entry.value().clone());
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_memtable_insert_get() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get(b"key2"), None);
    }

    #[test]
    fn test_memtable_overwrite() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec());
        memtable.insert(b"key1".to_vec(), b"value1_new".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value1_new".to_vec()));
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        memtable.delete(b"key1");
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn test_memtable_snapshot() {
        let memtable = MemTable::new();
        memtable.insert(b"k1".to_vec(), b"v1".to_vec());
        memtable.insert(b"k2".to_vec(), b"v2".to_vec());
        let snapshot = memtable.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(b"k1" as &[u8]), Some(&b"v1".to_vec()));
        assert_eq!(snapshot.get(b"k2" as &[u8]), Some(&b"v2".to_vec()));
    }

    #[test]
    fn test_memtable_concurrent_inserts() {
        let memtable = Arc::new(MemTable::new());
        let num_threads = 4;
        let num_inserts = 1000;
        let mut handles = vec![];

        for i in 0..num_threads {
            let mt = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for j in 0..num_inserts {
                    let key = format!("thread-{}-key-{}", i, j).into_bytes();
                    let value = format!("value-{}", j).into_bytes();
                    mt.insert(key, value);
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
                assert_eq!(memtable.get(&key), Some(expected_value));
            }
        }
    }
}
