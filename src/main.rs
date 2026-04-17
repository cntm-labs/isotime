#![cfg_attr(feature = "simd", feature(portable_simd))]
mod storage;

use crate::storage::compaction::Compactor;
use crate::storage::sstable::SSTable;
use crate::storage::StorageEngine;
use std::io;
use std::path::Path;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("isotime: High-Throughput Time-Series Engine starting...");

    // Initialize storage engine with WAL path
    let engine = StorageEngine::new("isotime.wal")?;

    // Demonstrate Put
    println!("Inserting data: hello -> world");
    engine.put(b"hello".to_vec(), b"world".to_vec())?;

    // Demonstrate Get
    if let Some(val) = engine.get(b"hello") {
        println!(
            "Recovered from MemTable: hello -> {:?}",
            String::from_utf8_lossy(&val)
        );
    }

    // Demonstrate Flush to SSTable 1
    println!("Flushing MemTable to SSTable: sst1.sst");
    engine.flush("sst1.sst")?;

    // Demonstrate second write and flush to SSTable 2
    println!("Inserting data: hello -> world_v2");
    engine.put(b"hello".to_vec(), b"world_v2".to_vec())?;
    println!("Inserting data: foo -> bar");
    engine.put(b"foo".to_vec(), b"bar".to_vec())?;
    println!("Flushing MemTable to SSTable: sst2.sst");
    engine.flush("sst2.sst")?;

    // Demonstrate SSTable Read (checking Bloom Filter)
    let sst2 = SSTable::open(Path::new("sst2.sst"))?;
    if let Some(val) = sst2.get(b"foo")? {
        println!(
            "Recovered from sst2.sst: foo -> {:?}",
            String::from_utf8_lossy(val)
        );
    }

    // Demonstrate Compaction (merging sst1 and sst2)
    println!("Compacting sst1.sst and sst2.sst into merged.sst...");
    Compactor::compact(
        &[Path::new("sst1.sst"), Path::new("sst2.sst")],
        Path::new("merged.sst"),
    )?;

    // Verify compacted result
    let merged = SSTable::open(Path::new("merged.sst"))?;
    if let Some(val) = merged.get(b"hello")? {
        println!(
            "Recovered from merged.sst: hello -> {:?}",
            String::from_utf8_lossy(val)
        );
    }
    if let Some(val) = merged.get(b"foo")? {
        println!(
            "Recovered from merged.sst: foo -> {:?}",
            String::from_utf8_lossy(val)
        );
    }

    // Demonstrate Delete
    println!("Deleting data from MemTable/WAL: hello");
    engine.delete(b"hello")?;

    if engine.get(b"hello").is_none() {
        println!("Verified: hello is deleted from MemTable.");
    }

    // Demonstrate all_entries (internal API check)
    let entries = merged.all_entries()?;
    println!("Compacted SSTable contains {} entries.", entries.len());

    println!("isotime: Engine shut down gracefully.");
    Ok(())
}
