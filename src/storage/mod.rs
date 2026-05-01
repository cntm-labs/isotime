pub mod bloom;
pub mod bus;
pub mod cas;
pub mod compaction;
pub mod compressor;
pub mod encryption;
pub mod memtable;
pub mod sstable;
pub mod tiering;
pub mod wal;

use crate::storage::bus::BusManager;
use crate::storage::cas::CASManager;
use crate::storage::compaction::Compactor;
use crate::storage::compressor::CompressionPolicy;
use crate::storage::encryption::EncryptionManager;
use crate::storage::memtable::MemTable;
use crate::storage::sstable::SSTable;
use crate::storage::tiering::{CapacityManager, SSTableMetadata, StorageTier};
use crate::storage::wal::{Wal, WalOp};
use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub struct StorageEngine {
    memtable: Arc<MemTable>,
    wal: Arc<Wal>,
    pub encryption: Option<Arc<EncryptionManager>>,
    pub policy: CompressionPolicy,
    pub cas: Arc<CASManager>,
    pub metadatas: Arc<Mutex<Vec<SSTableMetadata>>>,
    pub hot_dir: PathBuf,
    pub cold_dir: PathBuf,
}

impl StorageEngine {
    pub async fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        wal_path: P1,
        key: Option<[u8; 32]>,
        policy: CompressionPolicy,
        cas_root: P2,
    ) -> io::Result<Self> {
        let encryption = key.map(|k| Arc::new(EncryptionManager::new(&k)));
        let (wal, entries) = Wal::new(wal_path, encryption.clone()).await?;
        let memtable = MemTable::new();
        for entry in entries {
            match entry {
                WalOp::Put(key, value, tags) => memtable.insert(key, value, tags),
                WalOp::Delete(key) => memtable.insert(key, vec![], vec![]), // Recover as Tombstone
            }
        }

        let cas = Arc::new(CASManager::new(cas_root, encryption.clone())?);
        let metadatas = Arc::new(Mutex::new(Vec::new()));

        Ok(Self {
            memtable: Arc::new(memtable),
            wal: Arc::new(wal),
            encryption,
            policy,
            cas,
            metadatas,
            hot_dir: PathBuf::from("."),
            cold_dir: PathBuf::from("./cold"),
        })
    }

    pub fn spawn_background_tasks(self: Arc<Self>) {
        let engine = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(e) = engine.run_tiering_cycle().await {
                    eprintln!("Tiering cycle failed: {}", e);
                }
            }
        });
    }

    async fn run_tiering_cycle(&self) -> io::Result<()> {
        let mut metas_lock = self.metadatas.lock().await;

        // 1. TWCS Compaction
        for tier in [StorageTier::L0, StorageTier::L1] {
            let candidates = Compactor::get_merge_candidates(&metas_lock, tier);
            for group in candidates {
                let timestamp = group[0].window_start;
                let dest_name = format!("compacted_{:?}_{}.sst", tier, timestamp);
                let dest_path = self.hot_dir.join(dest_name);

                let policy = if tier == StorageTier::L1 {
                    CompressionPolicy::ExtremeSpace
                } else {
                    self.policy
                };

                match Compactor::compact(
                    &group,
                    &dest_path,
                    self.encryption.as_deref(),
                    policy,
                    Some(&self.cas),
                )
                .await
                {
                    Ok(new_meta) => {
                        let paths_to_remove: HashSet<_> =
                            group.iter().map(|m| m.path.clone()).collect();
                        metas_lock.retain(|m| !paths_to_remove.contains(&m.path));
                        for p in paths_to_remove {
                            let _ = std::fs::remove_file(p);
                        }
                        metas_lock.push(new_meta);
                    }
                    Err(e) => eprintln!("Compaction failed for {:?}: {}", tier, e),
                }
            }
        }

        // 2. Capacity Eviction (L2 -> L3)
        let cap_manager = CapacityManager {
            threshold: 0.85,
            hot_dir: self.hot_dir.clone(),
            cold_dir: self.cold_dir.clone(),
        };

        let eviction_candidates = cap_manager.find_eviction_candidates(&metas_lock);
        if !eviction_candidates.is_empty() {
            if !self.cold_dir.exists() {
                std::fs::create_dir_all(&self.cold_dir)?;
            }

            for meta_to_move in eviction_candidates {
                let filename = meta_to_move.path.file_name().unwrap();
                let new_path = self.cold_dir.join(filename);

                // Physically move file
                std::fs::rename(&meta_to_move.path, &new_path)?;

                // Update registry
                if let Some(m) = metas_lock.iter_mut().find(|m| m.path == meta_to_move.path) {
                    m.path = new_path.clone();
                    m.tier = StorageTier::L3;
                }
            }
        }

        Ok(())
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>, tags: Vec<String>) -> io::Result<()> {
        self.wal.append(&key, &value, tags.clone()).await?;
        self.memtable.insert(key, value, tags);
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 1. Check MemTable
        if let Some(val) = self.memtable.get(key) {
            if val.is_empty() {
                return Ok(None); // Tombstone found in MemTable
            }
            return Ok(Some(val));
        }

        // 2. Check SSTables (L0 -> L3, newest to oldest)
        let metas = {
            let guard = self.metadatas.lock().await;
            let mut m = guard.clone();
            // Sort by tier (L0 < L1 < L2 < L3) then by window_start descending
            m.sort_by(|a, b| {
                a.tier
                    .cmp(&b.tier)
                    .then_with(|| b.window_start.cmp(&a.window_start))
            });
            m
        };

        for meta in metas {
            let sstable = SSTable::open(&meta.path, self.encryption.as_deref()).await?;
            if let Some(val) = sstable.get(key, Some(&self.cas)).await? {
                if val.is_empty() {
                    return Ok(None); // Tombstone found in SSTable
                }
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    pub async fn get_by_tag(&self, tag: &str) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut keys = HashSet::new();

        // 1. Collect from MemTable
        for key in self.memtable.get_by_tag(tag) {
            keys.insert(key);
        }

        // 2. Collect from SSTables
        let metas = {
            let guard = self.metadatas.lock().await;
            guard.clone()
        };

        for meta in metas {
            let sstable = SSTable::open(&meta.path, self.encryption.as_deref()).await?;
            for key in sstable.get_by_tag(tag).await? {
                keys.insert(key);
            }
        }

        // 3. Resolve all keys to latest values
        let mut results = Vec::new();
        for key in keys {
            if let Some(val) = self.get(&key).await? {
                results.push((key, val));
            }
        }

        Ok(results)
    }

    pub async fn get_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged = BTreeMap::new();

        // 1. Get from SSTables (Oldest L3 to Newest L0)
        let metas = {
            let guard = self.metadatas.lock().await;
            let mut m = guard.clone();
            // Sort by tier descending (L3 -> L0), then window_start ascending (oldest -> newest)
            m.sort_by(|a, b| {
                b.tier
                    .cmp(&a.tier)
                    .then_with(|| a.window_start.cmp(&b.window_start))
            });
            m
        };

        for meta in metas {
            let sstable = SSTable::open(&meta.path, self.encryption.as_deref()).await?;
            let entries = sstable
                .get_range(start_key, end_key, Some(&self.cas))
                .await?;
            for (k, v) in entries {
                merged.insert(k, v);
            }
        }

        // 2. Get from MemTable (Newest)
        let mem_entries = self.memtable.get_range(start_key, end_key);
        for (k, v) in mem_entries {
            merged.insert(k, v);
        }

        // 3. Filter out tombstones (empty values)
        let final_results: Vec<_> = merged
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        Ok(final_results)
    }

    pub async fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.wal.delete(key).await?;
        self.memtable.insert(key.to_vec(), vec![], vec![]); // Insert empty vec as Tombstone
        Ok(())
    }

    pub async fn flush<P: AsRef<Path>>(&self, sstable_path: P) -> io::Result<()> {
        let (snapshot, tags) = self.memtable.snapshot();
        SSTable::write(
            sstable_path.as_ref(),
            snapshot,
            tags,
            self.encryption.as_deref(),
            self.policy,
            Some(&self.cas),
        )
        .await?;

        let now = SystemTime::now()
            .duration_since(UNISH_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        let meta = SSTableMetadata {
            path: sstable_path.as_ref().to_path_buf(),
            tier: StorageTier::L0,
            window_start: now,
            window_end: now,
            size_bytes: std::fs::metadata(sstable_path.as_ref())?.len(),
        };

        self.metadatas.lock().await.push(meta);
        Ok(())
    }

    pub async fn ingest_from_bus(&self, bus: &mut BusManager, limit: usize) -> io::Result<usize> {
        let batch = bus.pop_batch(limit);
        let count = batch.len();

        for event in batch {
            let key = event.event_id.to_le_bytes().to_vec();
            let mut value = vec![event.event_type];
            value.extend_from_slice(&event.timestamp.to_le_bytes());
            value.extend_from_slice(&event.payload);

            self.put(key, value, vec![]).await?;
        }

        Ok(count)
    }
}

const UNISH_EPOCH: SystemTime = UNIX_EPOCH;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::bus::DeltaEvent;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_tiering_cycle_integration() {
        tokio_uring::start(async {
            let wal_path = "test_tiering.wal";
            let cas_dir = tempdir().unwrap();
            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();

            // 1. Create multiple L0 files in same hour
            let now = SystemTime::now()
                .duration_since(UNISH_EPOCH)
                .unwrap()
                .as_secs();
            let base_hour = now / 3600 * 3600;

            for i in 0..3 {
                let path = format!("test_l0_{}.sst", i);
                let mut data = BTreeMap::new();
                data.insert(format!("key{}", i).into_bytes(), b"val".to_vec());
                SSTable::write(
                    Path::new(&path),
                    data,
                    BTreeMap::new(),
                    None,
                    CompressionPolicy::Balanced,
                    None,
                )
                .await
                .unwrap();

                let meta = SSTableMetadata {
                    path: PathBuf::from(&path),
                    tier: StorageTier::L0,
                    window_start: base_hour + i * 10,
                    window_end: base_hour + i * 10 + 5,
                    size_bytes: fs::metadata(&path).unwrap().len(),
                };
                engine.metadatas.lock().await.push(meta);
            }

            // 2. Run cycle -> Should merge L0s into L1
            engine.run_tiering_cycle().await.unwrap();

            let metas = engine.metadatas.lock().await;
            assert_eq!(metas.len(), 1);
            assert_eq!(metas[0].tier, StorageTier::L1);
            assert!(metas[0].path.to_string_lossy().contains("compacted_L0"));

            // Cleanup
            let _ = fs::remove_file(metas[0].path.clone());
            let _ = fs::remove_file(wal_path);
        });
    }

    #[test]
    fn test_storage_engine_put_get() {
        tokio_uring::start(async {
            let wal_path = "test_engine.wal";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            engine.put(b"key1".to_vec(), b"value1".to_vec(), vec![]).await.unwrap();
            assert_eq!(engine.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(engine.get(b"key2").await.unwrap(), None);

            let _ = fs::remove_file(wal_path);
        });
    }

    #[test]
    fn test_storage_engine_tag_search() {
        tokio_uring::start(async {
            let wal_path = "test_engine_tags.wal";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            
            engine.put(b"key1".to_vec(), b"v1".to_vec(), vec!["sensor1".to_string()]).await.unwrap();
            engine.put(b"key2".to_vec(), b"v2".to_vec(), vec!["sensor1".to_string()]).await.unwrap();
            engine.put(b"key3".to_vec(), b"v3".to_vec(), vec!["sensor2".to_string()]).await.unwrap();

            let results = engine.get_by_tag("sensor1").await.unwrap();
            assert_eq!(results.len(), 2);
            
            let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();
            assert!(keys.contains(&b"key1".to_vec()));
            assert!(keys.contains(&b"key2".to_vec()));

            let _ = fs::remove_file(wal_path);
        });
    }

    #[test]
    fn test_storage_engine_recovery() {
        tokio_uring::start(async {
            let wal_path = "test_recovery.wal";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }

            {
                let engine = StorageEngine::new(
                    wal_path,
                    None,
                    CompressionPolicy::Balanced,
                    cas_dir.path(),
                )
                .await
                .unwrap();
                engine.put(b"key1".to_vec(), b"value1".to_vec(), vec![]).await.unwrap();
                engine.put(b"key2".to_vec(), b"value2".to_vec(), vec![]).await.unwrap();
                engine.delete(b"key1").await.unwrap();
            }

            {
                let engine = StorageEngine::new(
                    wal_path,
                    None,
                    CompressionPolicy::Balanced,
                    cas_dir.path(),
                )
                .await
                .unwrap();
                assert_eq!(engine.get(b"key1").await.unwrap(), None);
                assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
            }

            let _ = fs::remove_file(wal_path);
        });
    }

    #[test]
    fn test_storage_engine_concurrent_put() {
        tokio_uring::start(async {
            let wal_path = "test_concurrent.wal";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }

            let engine = Arc::new(
                StorageEngine::new(
                    wal_path,
                    None,
                    CompressionPolicy::Balanced,
                    cas_dir.path(),
                )
                .await
                .unwrap(),
            );
            let num_threads = 4;
            let num_inserts = 50; 
            let mut handles = vec![];

            for i in 0..num_threads {
                let eng = Arc::clone(&engine);
                handles.push(tokio::spawn(async move {
                    for j in 0..num_inserts {
                        let key = format!("thread-{}-key-{}", i, j).into_bytes();
                        let value = format!("value-{}", j).into_bytes();
                        eng.put(key, value, vec![]).await.unwrap();
                    }
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            for i in 0..num_threads {
                for j in 0..num_inserts {
                    let key = format!("thread-{}-key-{}", i, j).into_bytes();
                    let expected_value = format!("value-{}", j).into_bytes();
                    assert_eq!(engine.get(&key).await.unwrap(), Some(expected_value));
                }
            }

            let _ = fs::remove_file(wal_path);
        });
    }

    #[test]
    fn test_storage_engine_flush() {
        tokio_uring::start(async {
            let wal_path = "test_flush.wal";
            let sst_path = "test_flush.sst";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }
            if Path::new(sst_path).exists() {
                let _ = fs::remove_file(sst_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            engine.put(b"k1".to_vec(), b"v1".to_vec(), vec![]).await.unwrap();
            engine.flush(sst_path).await.unwrap();

            let sstable = SSTable::open(Path::new(sst_path), None).await.unwrap();
            assert_eq!(
                sstable.get(b"k1", Some(&engine.cas)).await.unwrap(),
                Some(b"v1".to_vec())
            );

            // Verify metadata was recorded
            assert_eq!(engine.metadatas.lock().await.len(), 1);

            let _ = fs::remove_file(wal_path);
            let _ = fs::remove_file(sst_path);
        });
    }

    #[test]
    fn test_storage_engine_ingest_from_bus() {
        tokio_uring::start(async {
            let wal_path = "test_ingest.wal";
            let bus_path = "test_ingest_bus.bin";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }
            if Path::new(bus_path).exists() {
                let _ = fs::remove_file(bus_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            let mut bus = BusManager::new(bus_path, 10).unwrap();

            let event = DeltaEvent {
                event_id: 123,
                event_type: 1,
                _reserved: [0; 7],
                timestamp: 456,
                payload: [0xBB; 96],
                checksum: 0,
            };
            bus.push(event);

            let count = engine.ingest_from_bus(&mut bus, 10).await.unwrap();
            assert_eq!(count, 1);

            let key = 123u64.to_le_bytes().to_vec();
            let value = engine.get(&key).await.unwrap().unwrap();
            assert_eq!(value[0], 1);
            assert_eq!(&value[1..9], &456u64.to_le_bytes());
            assert_eq!(value[9], 0xBB);

            let _ = fs::remove_file(wal_path);
            let _ = fs::remove_file(bus_path);
        });
    }

    #[test]
    fn test_storage_engine_encryption() {
        tokio_uring::start(async {
            let wal_path = "test_engine_enc.wal";
            let sst_path = "test_engine_enc.sst";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }
            if Path::new(sst_path).exists() {
                let _ = fs::remove_file(sst_path);
            }

            let key = [0u8; 32];
            let engine = StorageEngine::new(
                wal_path,
                Some(key),
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            engine
                .put(b"secure_key".to_vec(), b"secure_value".to_vec(), vec![])
                .await
                .unwrap();
            engine.flush(sst_path).await.unwrap();

            // Read back with same key
            let engine2 = StorageEngine::new(
                "another.wal",
                Some(key),
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            let sstable = SSTable::open(Path::new(sst_path), engine2.encryption.as_deref()).await.unwrap();
            assert_eq!(
                sstable.get(b"secure_key", Some(&engine2.cas)).await.unwrap(),
                Some(b"secure_value".to_vec())
            );

            // Fail to read with wrong key
            let wrong_key = [1u8; 32];
            let engine3 = StorageEngine::new(
                "yet_another.wal",
                Some(wrong_key),
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();
            assert!(SSTable::open(Path::new(sst_path), engine3.encryption.as_deref()).await.is_err());

            let _ = fs::remove_file(wal_path);
            let _ = fs::remove_file(sst_path);
            let _ = fs::remove_file("another.wal");
            let _ = fs::remove_file("yet_another.wal");
        });
    }

    #[test]
    fn test_storage_engine_tombstone_flush() {
        tokio_uring::start(async {
            let wal_path = "test_tombstone.wal";
            let sst_path = "test_tombstone.sst";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }
            if Path::new(sst_path).exists() {
                let _ = fs::remove_file(sst_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();

            // Put and flush
            engine.put(b"k1".to_vec(), b"v1".to_vec(), vec![]).await.unwrap();
            engine.flush(sst_path).await.unwrap();

            // Delete (creates tombstone in memtable)
            engine.delete(b"k1").await.unwrap();

            // Should be None (tombstone masks the SSTable)
            assert_eq!(engine.get(b"k1").await.unwrap(), None);

            // Flush tombstone to new SSTable
            let sst2_path = "test_tombstone_2.sst";
            if Path::new(sst2_path).exists() {
                let _ = fs::remove_file(sst2_path);
            }
            engine.flush(sst2_path).await.unwrap();

            // Restart engine (to clear memtable)
            let engine2 = StorageEngine::new(
                "test_tombstone_new.wal",
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();

            // Manually load the metadatas for test
            let meta1 = SSTableMetadata {
                path: PathBuf::from(sst_path),
                tier: StorageTier::L0,
                window_start: 1,
                window_end: 1,
                size_bytes: 100,
            };
            let meta2 = SSTableMetadata {
                path: PathBuf::from(sst2_path),
                tier: StorageTier::L0,
                window_start: 2,
                window_end: 2,
                size_bytes: 100,
            };
            engine2.metadatas.lock().await.push(meta1);
            engine2.metadatas.lock().await.push(meta2);

            // Should be None because the newer SSTable has the tombstone
            assert_eq!(engine2.get(b"k1").await.unwrap(), None);

            let _ = fs::remove_file(wal_path);
            let _ = fs::remove_file(sst_path);
            let _ = fs::remove_file(sst2_path);
            let _ = fs::remove_file("test_tombstone_new.wal");
        });
    }

    #[test]
    fn test_storage_engine_get_range_with_tombstones() {
        tokio_uring::start(async {
            let wal_path = "test_engine_range.wal";
            let cas_dir = tempdir().unwrap();
            if Path::new(wal_path).exists() {
                let _ = fs::remove_file(wal_path);
            }

            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path(),
            )
            .await
            .unwrap();

            engine.put(b"k1".to_vec(), b"v1".to_vec(), vec![]).await.unwrap();
            engine.put(b"k2".to_vec(), b"v2".to_vec(), vec![]).await.unwrap();
            engine.put(b"k3".to_vec(), b"v3".to_vec(), vec![]).await.unwrap();

            // Flush to SSTable
            engine.flush("test_engine_range_1.sst").await.unwrap();

            // Override k2, Delete k3
            engine
                .put(b"k2".to_vec(), b"v2_new".to_vec(), vec![])
                .await
                .unwrap();
            engine.delete(b"k3").await.unwrap();

            let results = engine.get_range(b"k1", b"k4").await.unwrap();

            assert_eq!(results.len(), 2);
            assert_eq!(results[0], (b"k1".to_vec(), b"v1".to_vec()));
            assert_eq!(results[1], (b"k2".to_vec(), b"v2_new".to_vec())); // Updated
                                                                         // k3 is deleted, so it's missing

            let _ = fs::remove_file(wal_path);
            let _ = fs::remove_file("test_engine_range_1.sst");
        });
    }
}
