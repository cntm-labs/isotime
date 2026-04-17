# Specification: SHM Delta Streamer (The Observable Bus)

**Status:** Draft | **Date:** 2026-04-17
**Topic:** High-performance, zero-copy delta synchronization between `cntm-graph` and `isotime`.

## 1. Overview
The **SHM Delta Streamer** (internal name: "The Observable Bus") is a high-throughput communication layer designed to synchronize state changes (Deltas) from the live graph engine (`cntm-graph`) to the persistent time-series storage (`isotime`). It utilizes a lock-free ring buffer in Shared Memory (SHM) to achieve microsecond-level latency and zero-copy data transfer.

## 2. Goals
- **Performance:** Sub-microsecond overhead for event emission.
- **Reliability:** Per-slot checksums and backpressure management.
- **Observability:** Real-time monitoring of queue depth, throughput, and health.
- **Durability:** Ensuring every graph change is eventually persisted in the LSM-Tree.

## 3. Architecture & Memory Layout

### 3.1 Shared Memory Structure (`isotime_bus.bin`)
The memory mapping is divided into a **Global Header** and a **Data Region** containing fixed-size slots. All structures are 64-byte aligned to prevent CPU false sharing.

| Region | Component | Type | Description |
| :--- | :--- | :--- | :--- |
| **Header** | `magic_number` | `u64` | Identifier for the Bus protocol. |
| | `version` | `u32` | Schema version. |
| | `capacity` | `u32` | Number of slots in the ring buffer. |
| | `head` | `AtomicU64` | Current read position (owned by `isotime`). |
| | `tail` | `AtomicU64` | Current write position (owned by `cntm-graph`). |
| | `writer_pid` | `u32` | PID of the active writer process. |
| | `last_heartbeat`| `AtomicU64` | Unix timestamp of the last activity. |
| | `overflow_count`| `AtomicU64` | Counter for dropped/blocked events. |
| **Data** | `Slots[...]` | `[u8; 128]` | Array of 128-byte event structures. |

### 3.2 Delta Event Schema (Slot Layout)
Each slot is a fixed 128-byte block to enable zero-copy casting.

1.  **Event Header (16 bytes):**
    - `event_id`: `u64` (Monotonic sequence number)
    - `event_type`: `u8` (NodeAdd, NodeUpdate, NodeDelete, EdgeAdd, etc.)
    - `timestamp`: `u64` (Nanoseconds since epoch)
2.  - **Payload (104 bytes):**
    - **Node Event:** `node_id` (u64), `type_id` (u16), `weight` (f32).
    - **Edge Event:** `src_id` (u64), `tgt_id` (u64), `weight` (f32).
3.  - **Integrity (8 bytes):**
    - `checksum`: `u64` (CRC64 or similar of the first 120 bytes).

## 4. Mechanics

### 4.1 Lock-Free Operations
- **Writer (`cntm-graph`):** 
    1. Check if `tail - head < capacity`.
    2. If full: Enter **Backpressure** mode (spin-wait or yield).
    3. Reserve slot: `fetch_add` on `tail`.
    4. Write data + checksum.
    5. Update `last_heartbeat`.
- **Reader (`isotime`):**
    1. Poll `head` vs `tail`.
    2. Read batch of slots from `head`.
    3. Validate `checksum`.
    4. Forward valid events to LSM-Tree MemTable.
    5. Update `head`.

### 4.2 Auto-Recovery
- If `last_heartbeat` stalls for >10s while `writer_pid` is inactive, `isotime` logs a warning and marks the writer as disconnected.
- A dedicated utility `isotime-bus-reset` can force-reset `head` and `tail` to 0 in case of fatal corruption.

## 5. Tooling: `isotime-bus-top`
A CLI utility to monitor the bus health:
- **Metrics:** Current Lag (`tail - head`), Events/Sec, Checksum Errors, Overflow Rate.
- **Visuals:** A progress bar representing queue utilization.

## 6. Integration with LSM-Tree
Events are collected into batches (e.g., 1024 events). Batches are then:
1.  Sorted (if necessary for the LSM-Tree).
2.  Compressed using the **SIMD Delta-Delta** algorithm.
3.  Flushed into the MemTable.

## 7. Success Criteria
- [ ] Successful zero-copy transfer of 1M events/sec.
- [ ] `isotime-bus-top` correctly displays live metrics.
- [ ] Data consistency verified: Graph state at T1 matches SSTable state at T1.
