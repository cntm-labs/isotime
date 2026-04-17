# Design Spec: SIMD Delta Compressor with Hybrid De-duplication

## 1. Overview
The **SIMD Delta Compressor** is a high-performance compression and de-duplication engine for `isotime`. It aims to minimize storage growth for mixed time-series and graph data by combining adaptive SIMD-accelerated algorithms with content-addressable storage (CAS) principles during the compaction phase.

## 2. Goals
- **Maximize Storage Efficiency:** Reduce disk footprint for redundant graph nodes and time-series metrics.
- **High Throughput:** Leverage SIMD (AVX/SSE) for real-time inspection, hashing, and compression.
- **Zero-Touch Maintenance:** Implement "Intent-Based" policies that adapt automatically to data types.
- **Maintainability:** Standardize the decompression path to ensure long-term data accessibility.

## 3. Architecture & Components

### 3.1 Intent-Based Compression Policy
Users specify an overall goal rather than individual algorithms:
- `Fastest`: Minimal compression, maximum read/write speed.
- `Balanced` (Default): Good mix of speed and space saving.
- `ExtremeSpace`: Aggressive de-duplication and heavy SIMD compression.

### 3.2 SIMD Inspector
A background logic that "sniffs" data blocks during Compaction:
- **Integer/TS Detect:** Uses SIMD to check if data is predominantly monotonic integers (triggers Delta-Delta encoding).
- **Entropy Check:** Detects if data is highly random or structured (triggers LZ4-SIMD or Bit-packing).

### 3.3 Hybrid De-duplicator
Operates at two layers:
1. **Version Consolidation:** Removes obsolete versions of the same Key, keeping only the latest timestamped entry.
2. **Value Sharing (Value-Ref):**
    - Uses SIMD-accelerated hashing (e.g., HighwayHash) to identify identical Value payloads across different nodes.
    - Replaces duplicate values with a small `ValueRef` pointer (4-8 bytes) pointing to a single `RawValue` in the same SSTable block.

### 3.4 SIMD Algorithms
- **Delta-Delta Encoding:** For timestamps and counters.
- **Bit-packing:** Compresses low-variance integers into fewer bits.
- **LZ4-SIMD:** General-purpose block compression for mixed/text data.

## 4. Data Flow (Compaction Phase)
1. **Fetch:** Load multiple SSTables into memory buffers.
2. **Merge & Consolidate:** Identity identical Keys; use SIMD comparison to discard older versions.
3. **De-duplicate Values:** Compute hashes of all unique Values. If a match is found, assign a `ValueRef`.
4. **Inspect & Compress:** Analyze remaining unique values; apply the best SIMD algorithm based on the detected type and User Policy.
5. **Serialize:** Write the new optimized SSTable using the updated FlatBuffers schema.

## 5. Schema Changes (SSTable)
Update `src/storage/schema.fbs`:
- Add `ValueType` union: `Raw(vector<ubyte>)` or `Ref(uint32)`.
- Update `Entry` table to use `ValueType`.

## 6. Verification Plan
- **Unit Tests:** Verify each SIMD algorithm (Delta-Delta, LZ4) in isolation.
- **Integration Tests:** Simulate heavy node updates and verify that duplicate values result in predictable file size reduction.
- **Performance Benchmarks:** Measure Compression Ratio vs. CPU cycles across different `Intent` levels.

## 7. Future Considerations
- Support for cross-SSTable de-duplication (Global CAS).
- AVX-512 optimization paths when hardware becomes available.
