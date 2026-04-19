use isotime::storage::bus::{BusManager, DeltaEvent};
use isotime::storage::compaction::Compactor;
use isotime::storage::compressor::CompressionPolicy;
use isotime::storage::encryption::EncryptionManager;
use isotime::storage::sstable::SSTable;
use isotime::storage::StorageEngine;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("isotime: High-Throughput Time-Series Engine starting...");

    // Initialize encryption key
    let encryption_key = Some([0u8; 32]);
    let cas_root = "cas_store";

    // --- Demo: Intent-Based Compression Policies ---
    println!("\n--- Demo: Intent-Based Compression Policies ---");

    let policies = [
        ("Fastest", CompressionPolicy::Fastest),
        ("Balanced", CompressionPolicy::Balanced),
        ("ExtremeSpace", CompressionPolicy::ExtremeSpace),
    ];

    let mut data = BTreeMap::new();
    let redundant_val = b"this is a redundant value that will be shared across entries".to_vec();
    for i in 0..100 {
        data.insert(format!("key-{:03}", i).into_bytes(), redundant_val.clone());
    }

    let enc_manager_temp = encryption_key.map(|k| EncryptionManager::new(&k));

    for (name, policy) in policies {
        let path_str = format!("demo_{}.db", name.to_lowercase());
        let path = Path::new(&path_str);
        SSTable::write(path, data.clone(), enc_manager_temp.as_ref(), policy, None)?;
        let size = fs::metadata(path)?.len();
        println!("Policy: {:<12} | SSTable Size: {:>5} bytes", name, size);
        let _ = fs::remove_file(path);
    }

    // Initialize storage engine with Balanced policy
    let engine = StorageEngine::new(
        "isotime.wal",
        encryption_key,
        CompressionPolicy::Balanced,
        cas_root,
    )?;

    // --- Demo 1: Value Sharing (De-duplication) ---
    println!("\n--- Demo 1: Value Sharing (De-duplication) ---");
    for i in 0..50 {
        engine.put(format!("key-{:02}", i).into_bytes(), redundant_val.clone())?;
    }

    let shared_sst = "shared_values.db";
    engine.flush(shared_sst)?;
    let size = fs::metadata(shared_sst)?.len();
    println!("SSTable with 50 redundant entries size: {} bytes", size);

    // --- Demo 2: SIMD Delta-Delta Compression ---
    println!("\n--- Demo 2: SIMD Delta-Delta Compression ---");
    let mut timestamps = Vec::new();
    let mut t = 1713360000u64;
    for _ in 0..100 {
        timestamps.extend_from_slice(&t.to_le_bytes());
        t += 10;
    }

    engine.put(b"timeseries-data".to_vec(), timestamps.clone())?;
    let compressed_sst = "compressed_simd.db";
    engine.flush(compressed_sst)?;

    let enc_manager = engine.encryption.as_deref();
    let sst = SSTable::open(Path::new(compressed_sst), enc_manager)?;
    if let Some(val) = sst.get(b"timeseries-data", Some(&engine.cas))? {
        assert_eq!(val, timestamps);
        println!("SIMD Delta-Delta compression verified.");
    }

    // --- Demo 3: SHM Bus Ingestion ---
    println!("\n--- Demo 3: SHM Bus Ingestion ---");
    let mut bus = BusManager::new("bus.bin", 1024)?;
    for i in 0..10 {
        let event = DeltaEvent {
            event_id: 1000 + i as u64,
            event_type: (i % 3) as u8,
            _reserved: [0; 7],
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            payload: [i as u8; 96],
            checksum: 0,
        };
        bus.push(event);
    }
    let count = engine.ingest_from_bus(&mut bus, 100)?;
    println!("Ingested {} events from SHM Bus.", count);

    // --- Demo 4: Global CAS (Cross-SSTable De-duplication) ---
    println!("\n--- Demo 4: Global CAS ---");
    let global_val = b"global-cas-value-that-is-shared-across-files".to_vec();

    // Create engine with ExtremeSpace policy to trigger Global CAS
    let engine_extreme = StorageEngine::new(
        "extreme.wal",
        encryption_key,
        CompressionPolicy::ExtremeSpace,
        cas_root,
    )?;
    engine_extreme.put(b"cas-key-1".to_vec(), global_val.clone())?;
    engine_extreme.flush("cas_1.db")?;

    engine_extreme.put(b"cas-key-2".to_vec(), global_val.clone())?;
    engine_extreme.flush("cas_2.db")?;

    let size1 = fs::metadata("cas_1.db")?.len();
    let size2 = fs::metadata("cas_2.db")?.len();
    println!("SSTable 1 size: {} bytes", size1);
    println!("SSTable 2 size: {} bytes", size2);

    let cas_files: Vec<_> = fs::read_dir(cas_root)?.collect();
    println!("Global CAS objects count: {}", cas_files.len());

    // --- Demo 5: Compaction ---
    println!("\n--- Demo 5: Compaction ---");
    Compactor::compact(
        &[Path::new(shared_sst), Path::new(compressed_sst)],
        Path::new("final.db"),
        enc_manager,
        engine.policy,
        Some(&engine.cas),
    )?;
    let final_sst = SSTable::open(Path::new("final.db"), enc_manager)?;
    println!(
        "Final SSTable entry count: {}",
        final_sst.all_entries(Some(&engine.cas))?.len()
    );

    // Cleanup
    let _ = fs::remove_file("isotime.wal");
    let _ = fs::remove_file("extreme.wal");
    let _ = fs::remove_file(shared_sst);
    let _ = fs::remove_file(compressed_sst);
    let _ = fs::remove_file("cas_1.db");
    let _ = fs::remove_file("cas_2.db");
    let _ = fs::remove_file("final.db");
    let _ = fs::remove_file("bus.bin");
    let _ = fs::remove_dir_all(cas_root);

    println!("\nisotime: Engine shut down gracefully.");
    Ok(())
}
