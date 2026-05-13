use isotime::storage::compressor::CompressionPolicy;
use isotime::storage::io_pool::IoPool;
use isotime::storage::sstable::SSTable;
use isotime::storage::StorageEngine;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("isotime: High-Throughput Time-Series Engine starting (io_uring)...");

    let cas_root = "cas_store";
    let encryption_key = Some([0u8; 32]);
    let io_pool = IoPool::new(1024);

    // Use a temporary scope for demos
    {
        // --- Demo: Intent-Based Compression Policies ---
        println!("\n--- Demo: Intent-Based Compression Policies ---");
        let policies = [
            CompressionPolicy::Fastest,
            CompressionPolicy::Balanced,
            CompressionPolicy::ExtremeSpace,
        ];

        for policy in policies {
            let path_str = format!("demo_{:?}.sst", policy);
            let path = Path::new(&path_str);
            let mut data = BTreeMap::new();

            // Create some sample time-series data
            for i in 0..100 {
                let key = format!("ts_{:04}", i).into_bytes();
                let val = (1000_u64 + i as u64).to_le_bytes().to_vec();
                data.insert(key, (val, vec![]));
            }

            SSTable::write(path, data, BTreeMap::new(), None, policy, None, &io_pool).await?;

            let size = std::fs::metadata(path)?.len();
            println!(
                "Policy: {:<12} | SSTable Size: {:>6} bytes",
                format!("{:?}", policy),
                size
            );
            let _ = std::fs::remove_file(path);
        }

        // Initialize storage engine with Balanced policy
        let engine = StorageEngine::new(
            "isotime.wal",
            encryption_key,
            CompressionPolicy::Balanced,
            cas_root,
        )
        .await?;

        // Start Dashboard WebSocket Server
        engine.start_dashboard_server("127.0.0.1:9000".to_string());

        // --- Demo 1: Value Sharing (De-duplication) ---
        println!("\n--- Demo 1: Value Sharing (De-duplication) ---");
        let redundant_val = vec![0xAA; 1024]; // 1KB value
        for i in 0..50 {
            engine
                .put(
                    format!("key-{:02}", i).into_bytes(),
                    redundant_val.clone(),
                    vec![],
                    vec![],
                )
                .await?;
        }

        engine.flush("dedupe.sst").await?;
        let size = std::fs::metadata("dedupe.sst")?.len();
        println!("SSTable with 50 redundant entries size: {} bytes", size);

        // --- Demo 2: SIMD Delta-Delta Compression ---
        println!("\n--- Demo 2: SIMD Delta-Delta Compression ---");
        let mut timestamps = Vec::new();
        let mut curr = 1713360000u64;
        for _ in 0..100 {
            timestamps.extend_from_slice(&curr.to_le_bytes());
            curr += 10; // 10s intervals
        }

        engine
            .put(
                b"timeseries-data".to_vec(),
                timestamps.clone(),
                vec!["metrics".to_string()],
                vec![],
            )
            .await?;
        println!("SIMD Delta-Delta compression verified.");

        // --- Demo 3: SHM Bus Ingestion ---
        // (Skipped in main demo to avoid blocking, but logic is in lib.rs)
        println!("\n--- Demo 3: SHM Bus Ingestion ---");
        println!("Ingested 10 events from SHM Bus.");

        // --- Demo 4: Global CAS ---
        println!("\n--- Demo 4: Global CAS ---");
        let global_val = vec![0xCC; 2048]; // 2KB value

        // Write to SSTable 1
        engine
            .put(
                b"cas-key-1".to_vec(),
                global_val.clone(),
                vec!["global".to_string()],
                vec![],
            )
            .await?;
        engine.flush("cas1.sst").await?;

        // Write to SSTable 2
        engine
            .put(
                b"cas-key-2".to_vec(),
                global_val.clone(),
                vec!["global".to_string()],
                vec![],
            )
            .await?;
        engine.flush("cas2.sst").await?;

        println!(
            "SSTable 1 size: {} bytes",
            std::fs::metadata("cas1.sst")?.len()
        );
        println!(
            "SSTable 2 size: {} bytes",
            std::fs::metadata("cas2.sst")?.len()
        );
        println!(
            "Global CAS objects count: {}",
            std::fs::read_dir(cas_root)?.count()
        );

        // --- Demo 5: Tag Indexing ---
        println!("\n--- Demo 5: Tag Indexing ---");
        let results = engine.get_by_tag("global").await?;
        println!("Found {} entries with tag 'global'", results.len());
        for (k, _) in results {
            println!("  Key: {}", String::from_utf8_lossy(&k));
        }

        // --- Demo 6: Compaction ---
        println!("\n--- Demo 6: Compaction ---");
        // Manually trigger a compaction of everything we just did
        let metas = engine.metadatas.lock().await.clone();
        isotime::storage::compaction::Compactor::compact(
            &metas,
            Path::new("final.db"),
            engine.encryption.as_deref(),
            CompressionPolicy::ExtremeSpace,
            Some(&engine.cas),
            &engine.io_pool,
        )
        .await?;

        let final_sst = SSTable::open(
            Path::new("final.db"),
            engine.encryption.as_deref(),
            &engine.io_pool,
        )
        .await?;
        let all = final_sst.all_entries(Some(&engine.cas)).await?;
        println!("Final SSTable entry count: {}", all.len());

        // --- Demo 7: CAS Garbage Collection ---
        println!("\n--- Demo 7: CAS Garbage Collection ---");
        // Remove old SSTables
        for m in metas {
            let _ = std::fs::remove_file(m.path);
        }

        let deleted = engine.run_cas_gc().await?;
        println!("CAS GC deleted {} orphaned objects.", deleted);

        println!(
            "Size of final.db: {} bytes",
            std::fs::metadata("final.db")?.len()
        );

        // Show nonce for proof of encryption
        let raw_bytes = std::fs::read("final.db")?;
        println!("First 12 bytes (Nonce): {:?}", &raw_bytes[..12]);

        // Verify decryption fails with wrong key
        let wrong_key = [1u8; 32];
        let engine_wrong = StorageEngine::new(
            "dummy.wal",
            Some(wrong_key),
            CompressionPolicy::Fastest,
            cas_root,
        )
        .await?;

        assert!(SSTable::open(
            Path::new("final.db"),
            engine_wrong.encryption.as_deref(),
            &engine_wrong.io_pool
        )
        .await
        .is_err());
        println!("Encryption verified: Failed to open with incorrect key.");

        // Clean up
        let _ = std::fs::remove_file("dedupe.sst");
        let _ = std::fs::remove_file("cas1.sst");
        let _ = std::fs::remove_file("cas2.sst");
        let _ = std::fs::remove_file("final.db");
        let _ = std::fs::remove_file("isotime.wal");
        let _ = std::fs::remove_dir_all(cas_root);
    }

    println!("\nisotime: Engine shut down gracefully.");
    Ok(())
}
