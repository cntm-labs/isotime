#![feature(portable_simd)]
mod storage;

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

    // Demonstrate Flush to SSTable
    println!("Flushing MemTable to SSTable: snapshot.sst");
    engine.flush("snapshot.sst")?;

    // Demonstrate SSTable Read
    let sst = SSTable::open(Path::new("snapshot.sst"))?;
    if let Some(val) = sst.get(b"hello")? {
        println!(
            "Recovered from SSTable: hello -> {:?}",
            String::from_utf8_lossy(val)
        );
    }

    // Demonstrate Delete
    println!("Deleting data from MemTable/WAL: hello");
    engine.delete(b"hello")?;

    if engine.get(b"hello").is_none() {
        println!("Verified: hello is deleted from MemTable.");
    }

    println!("isotime: Engine shut down gracefully.");
    Ok(())
}
