use isotime::storage::bus::BusManager;
use std::fs;
use std::io::{self, Write};
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    let bus_path = "bus.bin";
    let bus = match BusManager::new(bus_path, 1024) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Failed to open bus at {}: {}", bus_path, e);
            return Err(e);
        }
    };

    println!("ISOTIME SHM Bus Monitor");
    println!("Path: {}", bus_path);
    println!("Press Ctrl+C to exit\n");

    loop {
        let header = bus.header();
        let head = header.head.load(std::sync::atomic::Ordering::Relaxed);
        let tail = header.tail.load(std::sync::atomic::Ordering::Relaxed);
        let lag = tail - head;
        let capacity = header.capacity;
        let overflow = header
            .overflow_count
            .load(std::sync::atomic::Ordering::Relaxed);
        let pid = header.writer_pid;

        // Scan storage
        let mut l0_size = 0;
        let mut l1_size = 0;
        let mut l2_size = 0;
        let mut l3_size = 0;

        if let Ok(entries) = fs::read_dir(".") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".sst") || name.ends_with(".db") {
                    let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    if name.contains("compacted_L0") {
                        l0_size += size;
                    } else if name.contains("compacted_L1") {
                        l1_size += size;
                    } else if name.contains("compacted_L2") {
                        l2_size += size;
                    } else {
                        l0_size += size; // Assume raw flushes are L0
                    }
                }
            }
        }

        if let Ok(entries) = fs::read_dir("./cold") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".sst") || name.ends_with(".db") {
                    let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    l3_size += size;
                }
            }
        }

        // Clear screen and reset cursor
        print!("\x1B[2J\x1B[H");
        println!("=== ISOTIME BUS TOP ===");
        println!("PID:      {}", pid);
        println!("Capacity: {}", capacity);
        println!("Head:     {}", head);
        println!("Tail:     {}", tail);
        println!("Lag:      {}", lag);
        println!("Overflow: {}", overflow);

        println!("\n=== STORAGE TIERING ===");
        println!(
            "[L0: {} KB | L1: {} KB | L2: {} KB | L3: {} KB]",
            l0_size / 1024,
            l1_size / 1024,
            l2_size / 1024,
            l3_size / 1024
        );

        // Progress bar
        let fill = (lag as f32 / capacity as f32 * 20.0) as usize;
        let bar = "#".repeat(fill);
        let empty = ".".repeat(20 - fill);
        println!(
            "\nBUS LAG: [{}{}] {:.1}%",
            bar,
            empty,
            (lag as f32 / capacity as f32 * 100.0)
        );

        io::stdout().flush()?;
        thread::sleep(Duration::from_millis(100));
    }
}
