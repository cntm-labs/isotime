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

[ [English](../README.md) | ภาษาไทย | [日本語](./README.ja.md) | [简体中文](./locales/README.zh.md) ]

> **Isotime: ชีพจรที่ไม่อาจเปลี่ยนแปลงของวิวัฒนาการความจำ AI**
ระบบจัดเก็บข้อมูลแบบ Time-series เฉพาะทางที่ออกแบบมาเพื่อบันทึกทุกวิวัฒนาการของ Knowledge Graph แบบเรียลไทม์ โดยทำหน้าที่เป็นชั้นความจำถาวรสำหรับ Chronos

## ✨ ฟีเจอร์เด่น (Features)
- 🚀 **การสตรีม Delta แบบ Zero-latency** — ผ่าน Shared Memory (SHM-Native) เพื่อการแพร่กระจายความรู้ในทันที
- 🛡️ **การบีบอัด Delta ระดับนาโนวินาที** — ด้วยการเร่งความเร็ว SIMD (AVX-512) เพื่อลดพื้นที่จัดเก็บโดยไม่เพิ่มความหน่วง
- 📊 **การท่องเวลาแบบสองทิศทางด้วยความเร็วสูง** — ปรับแต่งมาเพื่อการ Replay ประวัติศาสตร์และการให้เหตุผลเชิงสาเหตุ
- 🗄️ **Storage Engine ประสิทธิภาพสูง** — การใช้ LSM-Tree ร่วมกับ FlatBuffers (Zero-copy), Bloom Filters และกลยุทธ์ Time-Windowed Compaction

## 🛠️ เริ่มต้นใช้งาน (Quick Start)
```bash
cargo build --release && ./target/release/isotime --shm-path /dev/shm/cntm-graph
```

## 🗺️ การนำทาง (Navigation)
- 🏗️ **[สถาปัตยกรรม (Architecture)](../ARCHITECTURE.md)**
- 📅 **[แผนงาน (Roadmap)](../ROADMAP.md)**
- 🤝 **[การร่วมพัฒนา (Contributing)](../CONTRIBUTING.md)**
- 🌳 **[โครงสร้างโปรเจกต์ (Structure)](../STRUCTURE.tree)**

## ⚖️ ลิขสิทธิ์ (License)
[MIT](../LICENSE)
