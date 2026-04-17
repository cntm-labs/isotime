# System Architecture

## 🏗️ High-Level Overview
A specialized time-series storage designed to capture every evolution of the knowledge graph in real-time, serving as the permanent memory layer for Chronos.

## 🗺️ Component Diagram

graph TD
  subgraph Live_Cognition [cntm-graph - Live Engine]
    QG[Rust Graph Kernel]
    SHM[Shared Memory]
  end

  subgraph Temporal_Storage [isotime - Time-Series]
    Streaming[SHM Delta Streamer]
    LSM[Custom LSM-Tree]
    SIMD[SIMD Delta Compressor]
    Disk[(Encrypted SSTables)]
  end

  subgraph Reasoning_Layer [Chronos AI]
    Mojo[Mojo Inference]
  end

  QG <--> SHM
  SHM -- Zero-copy --> Streaming
  Streaming --> SIMD
  SIMD --> LSM
  LSM --> Disk
  Mojo -- Temporal Query --> LSM
  Disk -- Replay State --> QG

## 🗄️ LSM-Tree Details

The storage engine employs a custom Log-Structured Merge-Tree (LSM-Tree) optimized for high-throughput time-series data.

- **WAL (Write-Ahead Log):** All incoming deltas are first appended to a Write-Ahead Log to ensure durability and crash recovery.
- **MemTable:** An in-memory structure that provides low-latency write access. Once it reaches a certain threshold, it is frozen and flushed to disk as an SSTable.
- **SSTable (Sorted String Table):** On-disk storage format using **FlatBuffers** for zero-copy deserialization. Data is sorted by time and key for efficient range scans.
- **Bloom Filters:** Each SSTable is accompanied by a Bloom Filter to drastically reduce unnecessary disk reads by checking if a key potentially exists in a file before opening it.
- **Compaction (TWCS):** Utilizes a Time-Windowed Compaction Strategy to merge smaller SSTables into larger ones, maintaining high read performance and optimizing disk space.

## 🛠️ Technology Stack
- **Programming Languages:** Rust
- **Tooling & Infrastructure:** Tokio, io_uring, FlatBuffers, SIMD (AVX-512), LSM-Tree
- **Core Pattern:** Formal Immutability
- **Strategy:** Bridging active cognition with infinite historical context through causal-aware delta storage.

## 🔗 Internal References
- Engineering rules: [PRINCIPLES.md](PRINCIPLES.md)
- Live project map: [STRUCTURE.tree](STRUCTURE.tree)
