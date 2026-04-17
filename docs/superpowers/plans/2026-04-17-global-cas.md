# Global CAS (Cross-SSTable De-duplication) Implementation Plan

**Goal:** Implement a global Content-Addressable Storage (CAS) mechanism to de-duplicate identical values across different SSTables, significantly reducing storage usage for redundant graph and time-series data.

**Architecture:** 
- **Hash-Based ID:** Values are identified by their SHA-256 hash.
- **Global Store:** A dedicated `cas/` directory will store values named by their hash.
- **Lazy Loading:** SSTables will store the hash; the raw value is only fetched from the CAS store when requested.
- **Integration:** Hook into the write path (during flush/compaction) to check the global CAS store.

**Tech Stack:** Rust, `sha2`, FlatBuffers.

---

### Task 1: Schema & Foundation

**Files:**
- Modify: `src/storage/schema.fbs`
- Modify: `Cargo.toml`
- Create: `src/storage/cas.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Update schema.fbs to support Hash-Based references**
Add `HashValue` to the union.
```fbs
table HashValue {
  hash: [ubyte]; // 32 bytes for SHA-256
}

union ValueType { RawValue, RefValue, HashValue }
```

- [ ] **Step 2: Generate Rust code**
Run: `flatc --rust -o src/storage/ src/storage/schema.fbs`

- [ ] **Step 3: Add `sha2` dependency to Cargo.toml**

- [ ] **Step 4: Implement `CASManager` in `src/storage/cas.rs`**
Methods: `put(value) -> hash`, `get(hash) -> Option<value>`.

- [ ] **Step 5: Commit**
```bash
git add Cargo.toml src/storage/schema.fbs src/storage/schema_generated.rs src/storage/cas.rs src/storage/mod.rs
git commit -m "feat: implement Global CAS foundation and schema support"
```

---

### Task 2: Integrate CAS into SSTable Lifecycle

**Files:**
- Modify: `src/storage/sstable.rs`

- [ ] **Step 1: Update `SSTable::write` to support Global CAS**
When `ExtremeSpace` policy is active, check the global CAS store before writing `RawValue`.

- [ ] **Step 2: Update `SSTable::open` and `resolve_value` to fetch from CAS**
Handle `HashValue` type by querying the `CASManager`.

- [ ] **Step 3: Write test for cross-SSTable de-duplication**
```rust
#[test]
fn test_global_cas_deduplication() {
    // Write SSTable 1 with value X
    // Write SSTable 2 with same value X
    // Verify both point to same CAS entry
}
```

- [ ] **Step 4: Commit**
```bash
git add src/storage/sstable.rs
git commit -m "feat: integrate Global CAS into SSTable write/read paths"
```

---

### Task 3: Storage Engine Integration & Demo

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Update `StorageEngine` to own a `CASManager`**

- [ ] **Step 2: Update `main.rs` to demonstrate Global CAS savings**

- [ ] **Step 3: Final validation**
Run `cargo test`, `cargo fmt`, and `cargo clippy`.

- [ ] **Step 4: Commit**
```bash
git add src/storage/mod.rs src/main.rs
git commit -m "feat: finalize Global CAS integration"
```
