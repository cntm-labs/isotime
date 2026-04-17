### 📝 Description
This PR implements the core LSM-Tree Storage Engine for `isotime`. Key components include:
- **Write-Ahead Log (WAL):** For durability and crash recovery.
- **MemTable:** High-performance in-memory write buffer.
- **SSTables with FlatBuffers:** Zero-copy on-disk storage format.
- **Bloom Filters:** To minimize disk I/O for non-existent keys.
- **Time-Windowed Compaction Strategy (TWCS):** Optimized for time-series data.

### 🔗 Related Issues
Fixes #3

### 🛠️ Type of Change
- [x] 🚀 New feature
- [x] 📄 Documentation update
- [x] 🧪 Testing

### ✅ Checklist
- [x] Tests passed locally
- [x] Documentation updated
- [x] Code follows project style guidelines
- [x] Linked to the appropriate Issue
- [x] `cargo fmt` and `cargo clippy` pass

### 🏷️ Metadata
- **Assignee:** @mrbt
- **Labels:** `feat`, `docs`, `storage`
