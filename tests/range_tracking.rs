use isotime::storage::compressor::CompressionPolicy;
use isotime::storage::StorageEngine;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn test_numeric_range_tracking() {
    tokio_uring::start(async {
        let wal_path = "test_range.wal";
        let sst_path = "test_range.sst";
        let cas_dir = tempdir().unwrap();

        if Path::new(wal_path).exists() {
            let _ = fs::remove_file(wal_path);
        }
        if Path::new(sst_path).exists() {
            let _ = fs::remove_file(sst_path);
        }

        let engine =
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                .await
                .unwrap();

        // Put numeric data (f64 as 8 bytes)
        let val1 = 10.5f64.to_le_bytes().to_vec();
        let val2 = 50.2f64.to_le_bytes().to_vec();
        let val3 = 5.1f64.to_le_bytes().to_vec();

        engine
            .put(b"k1".to_vec(), val1, vec![], vec![])
            .await
            .unwrap();
        engine
            .put(b"k2".to_vec(), val2, vec![], vec![])
            .await
            .unwrap();
        engine
            .put(b"k3".to_vec(), val3, vec![], vec![])
            .await
            .unwrap();

        engine.flush(sst_path).await.unwrap();

        let metas = engine.metadatas.lock().await;
        assert_eq!(metas.len(), 1);
        let meta = &metas[0];

        assert_eq!(meta.min_val, Some(5.1));
        assert_eq!(meta.max_val, Some(50.2));

        let _ = fs::remove_file(wal_path);
        let _ = fs::remove_file(sst_path);
    });
}
