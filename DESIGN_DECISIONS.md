# Design Decisions (ADR)

## 💡 Philosophy
This project uses Architectural Decision Records (ADR) to track significant design choices.

## 📝 Decision Log

### ADR-001: Initial Scaffolding
- **Status:** Accepted
- **Context:** Bootstrapped using AGI Cognition Meta-Repo.
- **Decision:** Use Rust (Tokio/io_uring), Custom LSM-Tree, FlatBuffers (Zero-copy), SIMD (AVX-512) for the core implementation to balance performance and safety.
- **Consequences:** Provides a solid foundation for A specialized time-series storage designed to capture every evolution of the knowledge graph in real-time, serving as the permanent memory layer for Chronos.

---
*Add new decisions above this line using the standard ADR format.*
