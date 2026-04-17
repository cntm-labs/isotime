# SHM Delta Streamer (The Observable Bus) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance, lock-free Shared Memory (SHM) bus to synchronize graph deltas from `cntm-graph` to `isotime`.

**Architecture:** A ring buffer in SHM with a 64-byte aligned header and fixed-size (128-byte) slots. It uses atomic head/tail pointers for lock-free access and CRC64 for slot integrity, supported by a CLI monitor tool.

**Tech Stack:** Rust, `memmap2`, `crc64`, `core::simd`.

---

### Task 1: Core SHM Bus Layout & Structs

**Files:**
- Create: `src/storage/bus.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Define Event and Bus structures in `src/storage/bus.rs`**

```rust
use std::sync::atomic::AtomicU64;

#[repr(C, align(64))]
pub struct BusHeader {
    pub magic_number: u64,
    pub version: u32,
    pub capacity: u32,
    pub head: AtomicU64,
    pub tail: AtomicU64,
    pub writer_pid: u32,
    pub last_heartbeat: AtomicU64,
    pub overflow_count: AtomicU64,
}

#[repr(C, align(128))]
#[derive(Clone, Copy)]
pub struct DeltaEvent {
    pub event_id: u64,
    pub event_type: u8, // 0: NodeAdd, 1: NodeUpdate, 2: NodeDelete, 3: EdgeAdd, etc.
    pub timestamp: u64,
    pub payload: [u8; 104],
    pub checksum: u64,
}

impl Default for DeltaEvent {
    fn default() -> Self {
        Self {
            event_id: 0,
            event_type: 0,
            timestamp: 0,
            payload: [0; 104],
            checksum: 0,
        }
    }
}
```

- [ ] **Step 2: Add `bus` module to `src/storage/mod.rs`**
Add `pub mod bus;` to the top of the file.

- [ ] **Step 3: Commit**
```bash
git add src/storage/bus.rs src/storage/mod.rs
git commit -m "feat: define SHM Bus and Delta Event structures"
```

---

### Task 2: Bus Reader & Writer Mechanics

**Files:**
- Modify: `src/storage/bus.rs`

- [ ] **Step 1: Implement `Bus` manager for SHM mapping**

```rust
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

pub struct BusManager {
    mmap: MmapMut,
}

impl BusManager {
    pub const MAGIC: u64 = 0x49534F54494D4542; // "ISOTIMEB"
    
    pub fn new<P: AsRef<Path>>(path: P, capacity: u32) -> std::io::Result<Self> {
        let size = std::mem::size_of::<BusHeader>() + (capacity as usize * 128);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        
        // Initialize header if new
        let header = unsafe { &mut *(mmap.as_mut_ptr() as *mut BusHeader) };
        if header.magic_number != Self::MAGIC {
            header.magic_number = Self::MAGIC;
            header.version = 1;
            header.capacity = capacity;
            header.head.store(0, std::sync::atomic::Ordering::SeqCst);
            header.tail.store(0, std::sync::atomic::Ordering::SeqCst);
        }
        
        Ok(Self { mmap })
    }

    pub fn header(&self) -> &BusHeader {
        unsafe { &*(self.mmap.as_ptr() as *const BusHeader) }
    }

    pub fn slots_mut(&mut self) -> &mut [DeltaEvent] {
        let ptr = unsafe { self.mmap.as_mut_ptr().add(std::mem::size_of::<BusHeader>()) };
        unsafe { std::slice::from_raw_parts_mut(ptr as *mut DeltaEvent, self.header().capacity as usize) }
    }
}
```

- [ ] **Step 2: Add `push` and `pop_batch` methods to `BusManager`**
Implement logic for Atomic tail increment and head tracking.

- [ ] **Step 3: Write test for lock-free cycle**
```rust
#[test]
fn test_bus_push_pop_cycle() {
    let bus_path = "test_bus.bin";
    let mut bus = BusManager::new(bus_path, 1024).unwrap();
    // Push test event...
    // Pop and verify...
    std::fs::remove_file(bus_path).unwrap();
}
```

- [ ] **Step 4: Commit**
```bash
git add src/storage/bus.rs
git commit -m "feat: implement lock-free SHM Bus mechanics"
```

---

### Task 3: CLI Monitor Tool (`isotime-bus-top`)

**Files:**
- Create: `src/bin/bus_top.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Add new binary to `Cargo.toml`**

```toml
[[bin]]
name = "isotime-bus-top"
path = "src/bin/bus_top.rs"
```

- [ ] **Step 2: Implement TUI dashboard in `src/bin/bus_top.rs`**
Use `std::io::stdout` and simple clear-screen escapes to show Head, Tail, Lag, and EPS.

- [ ] **Step 3: Commit**
```bash
git add Cargo.toml src/bin/bus_top.rs
git commit -m "feat: add isotime-bus-top monitor tool"
```

---

### Task 4: Integration with LSM-Tree Ingestion

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Update `StorageEngine` to consume from Bus**
Add a background task that polls the `BusManager` and calls `engine.put()`.

- [ ] **Step 2: Update `main.rs` to demonstrate the Bus**
Simulate a writer pushing to the bus and the engine consuming it.

- [ ] **Step 3: Commit**
```bash
git add src/storage/mod.rs src/main.rs
git commit -m "feat: integrate SHM Bus with StorageEngine ingestion"
```

---

### Task 5: Final Validation & Auto-Recovery

**Files:**
- Modify: `src/storage/bus.rs`
- Modify: `ARCHITECTURE.md`

- [ ] **Step 1: Implement Heartbeat & Stale Writer detection**
Add logic to `BusManager` to check for stalled PIDs.

- [ ] **Step 2: Run full integration test**
`cntm-graph` simulator -> `SHM Bus` -> `isotime` -> `SSTable`.

- [ ] **Step 3: Update documentation**

- [ ] **Step 4: Commit**
```bash
git add src/storage/bus.rs ARCHITECTURE.md
git commit -m "docs: finalize SHM Streamer implementation"
```
