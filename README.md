<div align="center">

# isotime

**High-Throughput Time-Series Engine for Graph Deltas**

[![CI](https://github.com/cntm-labs/isotime/actions/workflows/ci.yml/badge.svg)](https://github.com/cntm-labs/isotime/actions/workflows/ci.yml)
[![Security](https://github.com/cntm-labs/isotime/actions/workflows/security.yml/badge.svg)](https://github.com/cntm-labs/isotime/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active-success)](./)

![Rust LOD](https://img.shields.io/badge/Rust_LOD-0-dea584.svg) ![Total LOD](https://img.shields.io/badge/Total_LOD-0-brightgreen.svg)

[![Rust](https://img.shields.io/badge/Rust-dea584?logo=rust&logoColor=white)](./) [![FlatBuffers](https://img.shields.io/badge/FlatBuffers-4285F4?logo=google&logoColor=white)](./) [![SIMD](https://img.shields.io/badge/SIMD-555555)](./) [![Tokio](https://img.shields.io/badge/Tokio-dea584?logo=rust&logoColor=white)](./)

</div>

---

[ English | [ภาษาไทย](./locales/README.th.md) | [日本語](./locales/README.ja.md) | [简体中文](./locales/README.zh.md) ]

A specialized time-series storage designed to capture every evolution of the knowledge graph in real-time, serving as the permanent memory layer for Chronos.

## ✨ Features

- 🚀 **Zero-Latency Delta Streaming** — Via Shared Memory (SHM-Native) for instant knowledge propagation.
- 🛡️ **Nano-Second Delta Compression** — Accelerated by SIMD (AVX-512) to minimize storage footprint without latency.
- 📊 **Bi-Directional High-Speed Traversal** — Optimized for historical replay and causal reasoning.
- 🗄️ **High-Performance Storage Engine** — LSM-Tree with Zero-copy FlatBuffers, Bloom Filters, and Time-Windowed Compaction.

## 🛠️ Quick Start

```bash
cargo build --release && ./target/release/isotime --shm-path /dev/shm/cntm-graph
```

## 🗺️ Navigation

- 🏗️ **[Architecture](ARCHITECTURE.md)** — Core design and components.
- 📅 **[Roadmap](ROADMAP.md)** — Project timeline and milestones.
- 🤝 **[Contributing](CONTRIBUTING.md)** — How to join and help.
- 🌳 **[Project Structure](STRUCTURE.tree)** — Full file map.

## ⚖️ License

[MIT](LICENSE)
