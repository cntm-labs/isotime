# SIMD Delta Compressor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance compression and de-duplication engine for `isotime` using SIMD and Value Sharing.

**Architecture:** 
1.  **Schema Update:** Add `ValueType` union to FlatBuffers to support raw bytes or references.
2.  **Value Sharing:** Implement block-local de-duplication during compaction using fast hashing.
3.  **SIMD Algorithms:** Implement Delta-Delta and LZ4-like compression with runtime CPU feature dispatch.
4.  **Integration:** Hook the compressor into the `Compactor` logic before SSTable serialization.

**Tech Stack:** Rust, FlatBuffers, `core::simd` (Nightly), `cupid` or `std::is_x86_feature_detected!`.

---

### Task 1: FlatBuffers Schema Evolution

**Files:**
- Modify: `src/storage/schema.fbs`
- Generate: `src/storage/schema_generated.rs`

- [ ] **Step 1: Update schema.fbs to support Value Sharing**
Add a union and update the Entry table.
```fbs
namespace isotime.storage;

table RawValue {
  data: [ubyte];
}

table RefValue {
  offset: uint32;
}

union ValueType { RawValue, RefValue }

table Entry {
  key: [ubyte];
  value: ValueType;
}

table SSTableData {
  entries: [Entry];
  bloom_filter: [ubyte];
  num_hashes: uint32;
}

root_type SSTableData;
```

- [ ] **Step 2: Generate Rust code**
Run: `flatc --rust -o src/storage/ src/storage/schema.fbs`

- [ ] **Step 3: Commit**
```bash
git add src/storage/schema.fbs src/storage/schema_generated.rs
git commit -m "feat: update FlatBuffers schema for Value Sharing"
```

---

### Task 2: Value Sharing Logic (De-duplication)

**Files:**
- Modify: `src/storage/sstable.rs`
- Modify: `src/storage/compaction.rs`

- [ ] **Step 1: Implement `ValueStore` helper in Compactor**
A simple hash-map based tracker to identify identical values within a compaction session.

- [ ] **Step 2: Update `SSTable::write` to handle `ValueType`**
Refactor the write logic to check if a value has been seen before; if so, write a `RefValue` instead of `RawValue`.

- [ ] **Step 3: Write test for de-duplication**
```rust
#[test]
fn test_sstable_value_sharing() {
    // Write 10 entries with identical values
    // Verify file size is small and entries point to RefValue
}
```

- [ ] **Step 4: Commit**
```bash
git add src/storage/sstable.rs src/storage/compaction.rs
git commit -m "feat: implement block-local Value Sharing de-duplication"
```

---

### Task 3: SIMD Algorithms & Runtime Dispatch

**Files:**
- Create: `src/storage/compressor.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Implement `DeltaDelta` SIMD algorithm**
Optimized for 64-bit timestamps/integers.

- [ ] **Step 2: Implement Runtime Dispatch**
Use `cfg_if` and `std::is_x86_feature_detected!` to select AVX2/SSE4.2 at runtime.

- [ ] **Step 3: Add unit tests for each compressor**
Test `compress` and `decompress` cycles for both Delta-Delta and a simple SIMD-accelerated bit-packing.

- [ ] **Step 4: Commit**
```bash
git add src/storage/compressor.rs src/storage/mod.rs
git commit -m "perf: add SIMD Delta-Delta compression with runtime dispatch"
```

---

### Task 4: Integration into Compaction Phase

**Files:**
- Modify: `src/storage/compaction.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Integrate Compressor into `Compactor::compact`**
Apply the compressor to unique values (RawValues) before they are serialized into the new SSTable.

- [ ] **Step 2: Implement Decompression in `SSTable::get`**
Update the read path to detect if a value is compressed and decompress it using SIMD before returning.

- [ ] **Step 3: Integration Test**
Full cycle: Write mixed data -> Compact with SIMD -> Read back and verify correctness.

- [ ] **Step 4: Commit**
```bash
git add src/storage/compaction.rs src/storage/sstable.rs
git commit -m "feat: integrate SIMD compression into Compaction and Read paths"
```

---

### Task 5: Documentation & Demo

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `src/main.rs`

- [ ] **Step 1: Update Architecture docs**
Add details about Value Sharing and SIMD Delta Compression.

- [ ] **Step 2: Update `main.rs` demo**
Demonstrate writing redundant data and showing the storage savings.

- [ ] **Step 3: Final validation**
Run `cargo test`, `cargo clippy`, and `cargo fmt`.

- [ ] **Step 4: Commit**
```bash
git add ARCHITECTURE.md src/main.rs
git commit -m "docs: finalize SIMD Compressor implementation and demo"
```
