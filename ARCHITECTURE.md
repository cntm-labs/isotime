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


## 🛠️ Technology Stack
- **Programming Languages:** Rust
- **Tooling & Infrastructure:** Tokio, io_uring, FlatBuffers, SIMD (AVX-512), LSM-Tree
- **Core Pattern:** Formal Immutability
- **Strategy:** Bridging active cognition with infinite historical context through causal-aware delta storage.

## 🔗 Internal References
- Engineering rules: [PRINCIPLES.md](PRINCIPLES.md)
- Live project map: [STRUCTURE.tree](STRUCTURE.tree)
