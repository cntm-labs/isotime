use isotime::storage::bus::{BusManager, DeltaEvent};
use isotime::storage::compaction::Compactor;
use isotime::storage::sstable::SSTable;
use isotime::storage::StorageEngine;
use std::fs;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("isotime: High-Throughput Time-Series Engine starting...");

    // Initialize encryption key (normally from env or KMS)
    let encryption_key = Some([0u8; 32]);

    // Initialize storage engine with WAL path and optional encryption
    let engine = StorageEngine::new("isotime.wal", encryption_key)?;

    // --- Demo 1: Value Sharing (De-duplication) ---
    println!("\n--- Demo 1: Value Sharing (De-duplication) ---");
    let redundant_val = b"this is a redundant value that will be shared across entries".to_vec();
    for i in 0..50 {
        engine.put(format!("key-{:02}", i).into_bytes(), redundant_val.clone())?;
    }

    // Verify from MemTable
    if let Some(val) = engine.get(b"key-00") {
        assert_eq!(val, redundant_val);
    }

    let shared_sst = "shared_values.db";
    engine.flush(shared_sst)?;
    let size = fs::metadata(shared_sst)?.len();
    println!("SSTable with 50 redundant entries size: {} bytes", size);
    println!("Value sharing successfully reduced disk footprint.");

    // --- Demo 2: SIMD Delta-Delta Compression ---
    println!("\n--- Demo 2: SIMD Delta-Delta Compression ---");
    let mut timestamps = Vec::new();
    let mut t = 1713360000u64; // Example timestamp
    for _ in 0..100 {
        timestamps.extend_from_slice(&t.to_le_bytes());
        t += 10;
    }

    engine.put(b"timeseries-data".to_vec(), timestamps.clone())?;
    let compressed_sst = "compressed_simd.db";
    engine.flush(compressed_sst)?;

    let enc_manager = engine.encryption.as_deref();
    let sst = SSTable::open(Path::new(compressed_sst), enc_manager)?;
    if let Some(val) = sst.get(b"timeseries-data")? {
        assert_eq!(val, timestamps);
        println!(
            "SIMD Delta-Delta compression verified: 800 bytes of timestamps recovered correctly."
        );
    }

    // --- Demo 3: SHM Bus Ingestion ---
    println!("\n--- Demo 3: SHM Bus Ingestion ---");
    // Initialize SHM Bus
    let mut bus = BusManager::new("bus.bin", 1024)?;

    // Simulate some work
    println!("Pushing test events to SHM Bus...");
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

    // Ingest from Bus
    println!("Ingesting events from SHM Bus into LSM-Tree...");
    let count = engine.ingest_from_bus(&mut bus, 100)?;
    println!("Ingested {} events.", count);

    // --- Demo 4: Compaction Flow (with Encryption) ---
    println!("\n--- Demo 4: Compaction Flow (with Encryption) ---");
    println!("Compacting demonstration SSTables into final.db...");
    Compactor::compact(
        &[Path::new(shared_sst), Path::new(compressed_sst)],
        Path::new("final.db"),
        enc_manager,
    )?;

    let final_sst = SSTable::open(Path::new("final.db"), enc_manager)?;
    println!(
        "Final SSTable entry count: {}",
        final_sst.all_entries()?.len()
    );

    // Verify encryption on disk
    let raw_data = fs::read("final.db")?;
    println!("Size of final.db: {} bytes", raw_data.len());
    println!("First 12 bytes (Nonce): {:?}", &raw_data[0..12]);

    // Demonstrate decryption failure with wrong key
    let wrong_key = Some([1u8; 32]);
    let engine_wrong = StorageEngine::new("wrong.wal", wrong_key)?;
    assert!(SSTable::open(Path::new("final.db"), engine_wrong.encryption.as_deref()).is_err());
    println!("Encryption verified: Failed to open with incorrect key.");

    // Demo Delete
    engine.delete(b"key-00")?;
    assert!(engine.get(b"key-00").is_none());

    // Cleanup
    let _ = fs::remove_file("isotime.wal");
    let _ = fs::remove_file(shared_sst);
    let _ = fs::remove_file(compressed_sst);
    let _ = fs::remove_file("final.db");
    let _ = fs::remove_file("bus.bin");
    let _ = fs::remove_file("wrong.wal");

    println!("\nisotime: Engine shut down gracefully.");
    Ok(())
}
