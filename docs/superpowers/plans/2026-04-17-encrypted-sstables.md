# Encrypted SSTables Implementation Plan

**Goal:** Implement authenticated encryption for SSTables on disk to ensure data confidentiality and integrity.

**Architecture:** 
- Use **AES-256-GCM-SIV** for high-performance, non-deterministic encryption with misuse resistance.
- Each SSTable will have a unique 12-byte nonce stored as a prefix in the file.
- The encryption key will be managed via an environment variable for now.

**Tech Stack:** Rust, `aes-gcm-siv`, `getrandom`.

---

### Task 1: Encryption Module Foundation

**Files:**
- Create: `src/storage/encryption.rs`
- Modify: `src/storage/mod.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Add dependencies to Cargo.toml**
Add `aes-gcm-siv` and `getrandom`.

- [ ] **Step 2: Implement `EncryptionManager` in `src/storage/encryption.rs`**
Implement `encrypt` and `decrypt` methods using a provided 32-byte key.

- [ ] **Step 3: Add `encryption` module to `src/storage/mod.rs`**

- [ ] **Step 4: Commit**
```bash
git add Cargo.toml src/storage/encryption.rs src/storage/mod.rs
git commit -m "feat: add encryption module with AES-GCM-SIV"
```

---

### Task 2: Integrate Encryption into SSTable Lifecycle

**Files:**
- Modify: `src/storage/sstable.rs`

- [ ] **Step 1: Update `SSTable::write` to encrypt output**
Generate a random nonce, encrypt the FlatBuffers data, and write `[nonce][ciphertext]` to the file.

- [ ] **Step 2: Update `SSTable::open` to decrypt input**
Read the nonce prefix, decrypt the remaining data, and load the resulting buffer.

- [ ] **Step 3: Write test for encrypted SSTable cycle**
```rust
#[test]
fn test_sstable_encryption_cycle() {
    // Write data with encryption
    // Read back and verify
    // Attempt to read with wrong key and verify failure
}
```

- [ ] **Step 4: Commit**
```bash
git add src/storage/sstable.rs
git commit -m "feat: integrate AES-GCM-SIV encryption into SSTable write/read"
```

---

### Task 3: Key Management & Integration

**Files:**
- Modify: `src/storage/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Update `StorageEngine` to handle encryption keys**
Store the encryption key in the engine and pass it to SSTable operations.

- [ ] **Step 2: Update `main.rs` to demonstrate encryption**
Show that data on disk is unreadable without the key.

- [ ] **Step 3: Final validation**
Run `cargo test`, `cargo fmt`, and `cargo clippy`.

- [ ] **Step 4: Commit**
```bash
git add src/storage/mod.rs src/main.rs
git commit -m "feat: finalize encrypted storage integration"
```
