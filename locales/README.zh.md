<div align="center">

# isotime

**High-Throughput Time-Series Engine for Graph Deltas**

[![CI](https://github.com/cntm-labs/isotime/actions/workflows/ci.yml/badge.svg)](https://github.com/cntm-labs/isotime/actions/workflows/ci.yml)
[![Security](https://github.com/cntm-labs/isotime/actions/workflows/security.yml/badge.svg)](https://github.com/cntm-labs/isotime/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-active-success)](./)

<!-- Language Badges: Synchronize with English README -->
![Rust](https://img.shields.io/badge/language-Rust-orange.svg) ![Mojo](https://img.shields.io/badge/language-Mojo-red.svg)

<!-- LOD Badges: Synchronize with English README -->
![Rust LOD](https://img.shields.io/badge/Rust_LOD-0-blue.svg) ![Total LOD](https://img.shields.io/badge/Total_LOD-0-brightgreen.svg)

</div>

---

[ [English](../README.md) | [ภาษาไทย](./README.th.md) | [日本語](./README.ja.md) | 简体中文 ]

> **Isotime: AI 记忆演变的永恒脉动**
专门的时间序列存储系统，旨在实时捕获知识图谱的每一次演变，作为 Chronos 的永久记忆层。

## ✨ 特性 (Features)
- 🚀 **零延迟 Delta 流传输** — 通过共享内存 (SHM-Native) 实现即时知识传播。
- 🛡️ **纳秒级 Delta 压缩** — 基于 SIMD (AVX-512) 加速，在不增加延迟的情况下最小化存储占用。
- 📊 **高速双向时间遍历** — 针对历史回放和因果推理进行了优化。

## 🛠️ 快速开始 (Quick Start)
```bash
cargo build --release && ./target/release/isotime --shm-path /dev/shm/cntm-graph
```

## 🗺️ 导航 (Navigation)
- 🏗️ **[架构 (Architecture)](../ARCHITECTURE.md)**
- 📅 **[路线图 (Roadmap)](../ROADMAP.md)**
- 🤝 **[贡献 (Contributing)](../CONTRIBUTING.md)**
- 🌳 **[项目结构 (Structure)](../STRUCTURE.tree)**

## ⚖️ 许可证 (License)
[MIT](../LICENSE)
