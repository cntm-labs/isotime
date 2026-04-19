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
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct StorageEngine {
    memtable: Arc<MemTable>,
    wal: Arc<Mutex<Wal>>,
    pub encryption: Option<Arc<EncryptionManager>>,
    pub policy: CompressionPolicy,
    pub cas: Arc<CASManager>,
    pub metadatas: Arc<Mutex<Vec<SSTableMetadata>>>,
    pub hot_dir: PathBuf,
    pub cold_dir: PathBuf,
}

impl StorageEngine {
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        wal_path: P1,
        key: Option<[u8; 32]>,
        policy: CompressionPolicy,
        cas_root: P2,
    ) -> io::Result<Self> {
        let wal = Wal::new(wal_path)?;
        let entries = wal.recover()?;
        let memtable = MemTable::new();
        for entry in entries {
            match entry {
                WalOp::Put(key, value) => memtable.insert(key, value),
                WalOp::Delete(key) => memtable.delete(&key),
            }
        }

        let encryption = key.map(|k| Arc::new(EncryptionManager::new(&k)));
        let cas = Arc::new(CASManager::new(cas_root)?);
        let metadatas = Arc::new(Mutex::new(Vec::new()));

        Ok(Self {
            memtable: Arc::new(memtable),
            wal: Arc::new(Mutex::new(wal)),
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
                if let Err(e) = engine.run_tiering_cycle() {
                    eprintln!("Tiering cycle failed: {}", e);
                }
            }
        });
    }

    fn run_tiering_cycle(&self) -> io::Result<()> {
        let mut metas = self
            .metadatas
            .lock()
            .map_err(|_| io::Error::other("Lock poisoned"))?;

        // 1. TWCS Compaction
        for tier in [StorageTier::L0, StorageTier::L1] {
            let candidates = Compactor::get_merge_candidates(&metas, tier);
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
                ) {
                    Ok(new_meta) => {
                        let paths_to_remove: HashSet<_> =
                            group.iter().map(|m| m.path.clone()).collect();
                        metas.retain(|m| !paths_to_remove.contains(&m.path));
                        for p in paths_to_remove {
                            let _ = std::fs::remove_file(p);
                        }
                        metas.push(new_meta);
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

        let eviction_candidates = cap_manager.find_eviction_candidates(&metas);
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
                if let Some(m) = metas.iter_mut().find(|m| m.path == meta_to_move.path) {
                    m.path = new_path.clone();
                    m.tier = StorageTier::L3;
                }
            }
        }

        Ok(())
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

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Ok(self.memtable.get(key))
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
        SSTable::write(
            sstable_path.as_ref(),
            snapshot,
            self.encryption.as_deref(),
            self.policy,
            Some(&self.cas),
        )?;

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

        self.metadatas.lock().unwrap().push(meta);
        Ok(())
    }

    pub fn ingest_from_bus(&self, bus: &mut BusManager, limit: usize) -> io::Result<usize> {
        let batch = bus.pop_batch(limit);
        let count = batch.len();

        for event in batch {
            let key = event.event_id.to_le_bytes().to_vec();
            let mut value = vec![event.event_type];
            value.extend_from_slice(&event.timestamp.to_le_bytes());
            value.extend_from_slice(&event.payload);

            self.put(key, value)?;
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
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_tiering_cycle_integration() {
        let wal_path = "test_tiering.wal";
        let cas_dir = tempdir().unwrap();
        let engine =
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
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
                None,
                CompressionPolicy::Balanced,
                None,
            )
            .unwrap();

            let meta = SSTableMetadata {
                path: PathBuf::from(&path),
                tier: StorageTier::L0,
                window_start: base_hour + i * 10,
                window_end: base_hour + i * 10 + 5,
                size_bytes: fs::metadata(&path).unwrap().len(),
            };
            engine.metadatas.lock().unwrap().push(meta);
        }

        // 2. Run cycle -> Should merge L0s into L1
        engine.run_tiering_cycle().unwrap();

        let metas = engine.metadatas.lock().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].tier, StorageTier::L1);
        assert!(metas[0].path.to_string_lossy().contains("compacted_L0"));

        // Cleanup
        let _ = fs::remove_file(metas[0].path.clone());
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn test_storage_engine_put_get() {
        let wal_path = "test_engine.wal";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let engine =
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                .unwrap();
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), None);

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_recovery() {
        let wal_path = "test_recovery.wal";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        {
            let engine =
                StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                    .unwrap();
            engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
            engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
            engine.delete(b"key1").unwrap();
        }

        {
            let engine =
                StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                    .unwrap();
            assert_eq!(engine.get(b"key1").unwrap(), None);
            assert_eq!(engine.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_concurrent_put() {
        let wal_path = "test_concurrent.wal";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let engine = Arc::new(
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                .unwrap(),
        );
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
                assert_eq!(engine.get(&key).unwrap(), Some(expected_value));
            }
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_storage_engine_flush() {
        let wal_path = "test_flush.wal";
        let sst_path = "test_flush.sst";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }
        if Path::new(sst_path).exists() {
            fs::remove_file(sst_path).unwrap();
        }

        let engine =
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                .unwrap();
        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.flush(sst_path).unwrap();

        let sstable = SSTable::open(Path::new(sst_path), None).unwrap();
        assert_eq!(
            sstable.get(b"k1", Some(&engine.cas)).unwrap(),
            Some(b"v1".to_vec())
        );

        // Verify metadata was recorded
        assert_eq!(engine.metadatas.lock().unwrap().len(), 1);

        fs::remove_file(wal_path).unwrap();
        fs::remove_file(sst_path).unwrap();
    }

    #[test]
    fn test_storage_engine_ingest_from_bus() {
        let wal_path = "test_ingest.wal";
        let bus_path = "test_ingest_bus.bin";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }
        if Path::new(bus_path).exists() {
            fs::remove_file(bus_path).unwrap();
        }

        let engine =
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
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

        let count = engine.ingest_from_bus(&mut bus, 10).unwrap();
        assert_eq!(count, 1);

        let key = 123u64.to_le_bytes().to_vec();
        let value = engine.get(&key).unwrap().unwrap();
        assert_eq!(value[0], 1);
        assert_eq!(&value[1..9], &456u64.to_le_bytes());
        assert_eq!(value[9], 0xBB);

        fs::remove_file(wal_path).unwrap();
        fs::remove_file(bus_path).unwrap();
    }

    #[test]
    fn test_storage_engine_encryption() {
        let wal_path = "test_engine_enc.wal";
        let sst_path = "test_engine_enc.sst";
        let cas_dir = tempdir().unwrap();
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }
        if Path::new(sst_path).exists() {
            fs::remove_file(sst_path).unwrap();
        }

        let key = [0u8; 32];
        let engine = StorageEngine::new(
            wal_path,
            Some(key),
            CompressionPolicy::Balanced,
            cas_dir.path(),
        )
        .unwrap();
        engine
            .put(b"secure_key".to_vec(), b"secure_value".to_vec())
            .unwrap();
        engine.flush(sst_path).unwrap();

        // Read back with same key
        let engine2 = StorageEngine::new(
            "another.wal",
            Some(key),
            CompressionPolicy::Balanced,
            cas_dir.path(),
        )
        .unwrap();
        let sstable = SSTable::open(Path::new(sst_path), engine2.encryption.as_deref()).unwrap();
        assert_eq!(
            sstable.get(b"secure_key", Some(&engine2.cas)).unwrap(),
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
        .unwrap();
        assert!(SSTable::open(Path::new(sst_path), engine3.encryption.as_deref()).is_err());

        fs::remove_file(wal_path).unwrap();
        fs::remove_file(sst_path).unwrap();
        let _ = fs::remove_file("another.wal");
        let _ = fs::remove_file("yet_another.wal");
    }
}
