use isotime::storage::bus::BusManager;
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

        // Clear screen and reset cursor
        print!("\x1B[2J\x1B[H");
        println!("=== ISOTIME BUS TOP ===");
        println!("PID:      {}", pid);
        println!("Capacity: {}", capacity);
        println!("Head:     {}", head);
        println!("Tail:     {}", tail);
        println!("Lag:      {}", lag);
        println!("Overflow: {}", overflow);

        // Progress bar
        let fill = (lag as f32 / capacity as f32 * 20.0) as usize;
        let bar = "#".repeat(fill);
        let empty = ".".repeat(20 - fill);
        println!(
            "[{}{}] {:.1}%",
            bar,
            empty,
            (lag as f32 / capacity as f32 * 100.0)
        );

        io::stdout().flush()?;
        thread::sleep(Duration::from_millis(100));
    }
}
