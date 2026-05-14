use isotime::storage::compressor::CompressionPolicy;
use isotime::storage::StorageEngine;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn test_causal_query_engine_integration() {
    tokio_uring::start(async {
        let wal_path = "test_query_engine.wal";
        let sst_path = "test_query_engine.sst";
        let cas_dir = tempdir().unwrap();

        if Path::new(wal_path).exists() {
            let _ = fs::remove_file(wal_path);
        }
        if Path::new(sst_path).exists() {
            let _ = fs::remove_file(sst_path);
        }

        let engine = std::sync::Arc::new(
            StorageEngine::new(wal_path, None, CompressionPolicy::Balanced, cas_dir.path())
                .await
                .unwrap(),
        );

        // 1. Insert data with vector clocks
        // T1: Node 1 sets value 100.0 (Counter 1)
        let val100 = 100.0f64.to_le_bytes().to_vec();
        engine
            .put(
                b"sensor1".to_vec(),
                val100.clone(),
                vec!["temp".to_string()],
                vec![(1, 1)],
            )
            .await
            .unwrap();

        // T2: Node 1 updates to 105.0 (Counter 2) - Happens After T1
        let val105 = 105.0f64.to_le_bytes().to_vec();
        engine
            .put(
                b"sensor1".to_vec(),
                val105.clone(),
                vec!["temp".to_string()],
                vec![(1, 2)],
            )
            .await
            .unwrap();

        // T3: Node 2 sets value 90.0 (Counter 1) - Concurrent with T1/T2
        let val90 = 90.0f64.to_le_bytes().to_vec();
        engine
            .put(
                b"sensor2".to_vec(),
                val90.clone(),
                vec!["temp".to_string()],
                vec![(2, 1)],
            )
            .await
            .unwrap();

        // Flush to SSTable to test parallel scan
        engine.flush(sst_path).await.unwrap();

        // T4: In MemTable, Node 1 updates again to 110.0 (Counter 3)
        let val110 = 110.0f64.to_le_bytes().to_vec();
        engine
            .put(
                b"sensor1".to_vec(),
                val110.clone(),
                vec!["temp".to_string()],
                vec![(1, 3)],
            )
            .await
            .unwrap();

        // Query 1: All "temp" tags
        let results = engine.query().tag("temp").execute().await;
        assert_eq!(results.len(), 2); // sensor1 and sensor2

        // Query 2: Values > 100.0
        let results = engine
            .query()
            .tag("temp")
            .range(101.0, 200.0)
            .execute()
            .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"sensor1");
        assert_eq!(results[0].1, val110);

        // Query 3: Causal After Node 1, Counter 1
        let results = engine.query().after(vec![(1, 1)]).execute().await;
        // Should include sensor1 (Counter 3 > 1)
        // Should NOT include sensor2 (Node 2 Counter 1 is concurrent/not greater than Node 1 Counter 1)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"sensor1");

        // Query 4: Causal After Node 1, Counter 5 (None should match)
        let results = engine.query().after(vec![(1, 5)]).execute().await;
        assert_eq!(results.len(), 0);

        // Cleanup
        let _ = fs::remove_file(wal_path);
        let _ = fs::remove_file(sst_path);
    });
}
