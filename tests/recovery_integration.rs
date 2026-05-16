use isotime::storage::StorageEngine;
use isotime::storage::compressor::CompressionPolicy;
use tempfile::tempdir;
use std::fs;
use std::path::Path;

#[test]
fn test_manifest_recovery_persistence() {
    tokio_uring::start(async {
        let wal_path = "recovery.wal";
        let sst_path = "recovery.sst";
        // The engine now uses wal_path.with_extension("manifest.json")
        let manifest_path = "recovery.manifest.json";
        let cas_dir = tempdir().unwrap();

        // Cleanup
        let _ = fs::remove_file(wal_path);
        let _ = fs::remove_file(sst_path);
        let _ = fs::remove_file(manifest_path);

        {
            let engine = StorageEngine::new(
                wal_path,
                None,
                CompressionPolicy::Balanced,
                cas_dir.path()
            ).await.unwrap();

            engine.put(b"k1".to_vec(), b"v1".to_vec(), vec![], vec![]).await.unwrap();
            engine.flush(sst_path).await.unwrap();
            
            // At this point, manifest should be saved
            assert!(Path::new(manifest_path).exists(), "Manifest file {} should exist", manifest_path);
            
            // Verify data is there
            assert_eq!(engine.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        }

        // Engine is dropped. Now start a NEW engine and see if it recovers k1 from SSTable via Manifest.
        {
            // We MUST use the same WAL path (or at least same manifest path logic)
            // Actually, to test manifest recovery, we need a path that maps to the SAME manifest.
            // If we use "recovery.wal" again, it will find "recovery.manifest.json".
            let engine = StorageEngine::new(
                "recovery.wal", 
                None,
                CompressionPolicy::Balanced,
                cas_dir.path()
            ).await.unwrap();

            // Should recover 1 metadata entry
            assert_eq!(engine.metadatas.lock().await.len(), 1);

            // Should find k1
            assert_eq!(engine.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        }

        // Cleanup
        let _ = fs::remove_file(wal_path);
        let _ = fs::remove_file(sst_path);
        let _ = fs::remove_file(manifest_path);
    });
}
