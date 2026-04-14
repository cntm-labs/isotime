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

[ [English](../README.md) | [ภาษาไทย](./README.th.md) | 日本語 | [简体中文](./README.zh.md) ]

> **Isotime: AIメモリ進化の不変の鼓動**
ナレッジグラフのあらゆる進化をリアルタイムで捉えるために設計された、特殊なタイムシリーズ・ストレージ。Chronosの永続メモリレイヤーとして機能します。

## ✨ 特徴 (Features)
- 🚀 **ゼロレイテンシ・デルタストリーミング** — 共有メモリ(SHM)ネイティブによる即時知識伝搬。
- 🛡️ **ナノ秒単位のデルタ圧縮** — SIMD (AVX-512) 加速により、遅延なしでストレージ使用量を最小限に。
- 📊 **高速双方向タイムトラバーサル** — 履歴再生と因果推論のために最適化。

## 🛠️ クイックスタート (Quick Start)
```bash
cargo build --release && ./target/release/isotime --shm-path /dev/shm/cntm-graph
```

## 🗺️ ナビゲーション (Navigation)
- 🏗️ **[アーキテクチャ (Architecture)](../ARCHITECTURE.md)**
- 📅 **[ロードマップ (Roadmap)](../ROADMAP.md)**
- 🤝 **[貢献する (Contributing)](../CONTRIBUTING.md)**
- 🌳 **[プロジェクト構造 (Structure)](../STRUCTURE.tree)**

## ⚖️ ライセンス (License)
[MIT](../LICENSE)
