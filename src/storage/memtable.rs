use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use std::sync::Arc;

pub struct MemTable {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    tag_index: Arc<DashMap<String, Vec<Vec<u8>>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            tag_index: Arc::new(DashMap::new()),
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>, tags: Vec<String>) {
        self.map.insert(key.clone(), value);
        for tag in tags {
            self.tag_index.entry(tag).or_default().push(key.clone());
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn get_by_tag(&self, tag: &str) -> Vec<Vec<u8>> {
        if let Some(entry) = self.tag_index.get(tag) {
            entry.value().clone()
        } else {
            Vec::new()
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.map.remove(key);
        // We don't remove from tag_index to keep it simple (LSM-style)
        // StorageEngine::get will handle resolving if the key still exists.
    }

    pub fn snapshot(
        &self,
    ) -> (
        std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
        std::collections::BTreeMap<String, Vec<Vec<u8>>>,
    ) {
        let mut snapshot = std::collections::BTreeMap::new();
        for entry in self.map.iter() {
            snapshot.insert(entry.key().clone(), entry.value().clone());
        }

        let mut tags = std::collections::BTreeMap::new();
        for entry in self.tag_index.iter() {
            tags.insert(entry.key().clone(), entry.value().clone());
        }

        (snapshot, tags)
    }

    pub fn get_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();
        // Crossbeam SkipMap range is inclusive on start, exclusive on end by default
        for entry in self.map.range(start_key.to_vec()..end_key.to_vec()) {
            results.push((entry.key().clone(), entry.value().clone()));
        }
        results
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
        memtable.insert(b"key1".to_vec(), b"value1".to_vec(), vec!["t1".to_string()]);
        assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(memtable.get_by_tag("t1"), vec![b"key1".to_vec()]);
        assert_eq!(memtable.get(b"key2"), None);
    }

    #[test]
    fn test_memtable_overwrite() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec(), vec![]);
        memtable.insert(b"key1".to_vec(), b"value2".to_vec(), vec![]);
        assert_eq!(memtable.get(b"key1"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_memtable_delete() {
        let memtable = MemTable::new();
        memtable.insert(b"key1".to_vec(), b"value1".to_vec(), vec![]);
        memtable.delete(b"key1");
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn test_memtable_snapshot() {
        let memtable = MemTable::new();
        memtable.insert(b"b".to_vec(), b"2".to_vec(), vec!["tag".to_string()]);
        memtable.insert(b"a".to_vec(), b"1".to_vec(), vec![]);
        memtable.insert(b"c".to_vec(), b"3".to_vec(), vec!["tag".to_string()]);

        let (snapshot, tags) = memtable.snapshot();
        let keys: Vec<_> = snapshot.keys().collect();
        assert_eq!(keys, vec![&b"a".to_vec(), &b"b".to_vec(), &b"c".to_vec()]);
        assert_eq!(tags.get("tag").unwrap().len(), 2);
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
                    m.insert(key, val, vec![]);
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

    #[test]
    fn test_memtable_get_range() {
        let memtable = MemTable::new();
        memtable.insert(b"k1".to_vec(), b"v1".to_vec(), vec![]);
        memtable.insert(b"k2".to_vec(), b"v2".to_vec(), vec![]);
        memtable.insert(b"k3".to_vec(), b"v3".to_vec(), vec![]);
        memtable.insert(b"k4".to_vec(), b"v4".to_vec(), vec![]);

        let results = memtable.get_range(b"k2", b"k4");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"k2");
        assert_eq!(results[1].0, b"k3");
    }
}
