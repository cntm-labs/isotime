use crossbeam_skiplist::SkipMap;
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

    pub fn snapshot(&self) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
        let mut snapshot = std::collections::BTreeMap::new();
        for entry in self.map.iter() {
            snapshot.insert(entry.key().clone(), entry.value().clone());
        }
        snapshot
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
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
        memtable.insert(b"key1".to_vec(), b"value2".to_vec());
        assert_eq!(memtable.get(b"key1"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec());
        memtable.delete(b"key1");
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn test_memtable_snapshot() {
        let memtable = MemTable::new();
        memtable.insert(b"b".to_vec(), b"2".to_vec());
        memtable.insert(b"a".to_vec(), b"1".to_vec());
        memtable.insert(b"c".to_vec(), b"3".to_vec());

        let snapshot = memtable.snapshot();
        let keys: Vec<_> = snapshot.keys().collect();
        assert_eq!(keys, vec![&b"a".to_vec(), &b"b".to_vec(), &b"c".to_vec()]);
    }

    #[test]
    fn test_memtable_concurrent_inserts() {
        let memtable = Arc::new(MemTable::new());
        let mut handles = vec![];

        for i in 0..10 {
            let m = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("thread-{}-key-{}", i, j).into_bytes();
                    let val = format!("val-{}", j).into_bytes();
                    m.insert(key, val);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10 {
            for j in 0..100 {
                let key = format!("thread-{}-key-{}", i, j).into_bytes();
                assert!(memtable.get(&key).is_some());
            }
        }
    }
}
