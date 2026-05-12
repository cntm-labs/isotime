use crate::storage::encryption::EncryptionManager;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

pub struct CASManager {
    root: PathBuf,
    encryption: Option<Arc<EncryptionManager>>,
    ref_counts: RwLock<HashMap<[u8; 32], usize>>,
}

impl CASManager {
    pub fn new<P: AsRef<Path>>(
        root: P,
        encryption: Option<Arc<EncryptionManager>>,
    ) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        if !root.exists() {
            std::fs::create_dir_all(&root)?;
        }
        Ok(Self {
            root,
            encryption,
            ref_counts: RwLock::new(HashMap::new()),
        })
    }

    /// Registers a reference to a hash.
    pub async fn register_ref(&self, hash: [u8; 32]) {
        let mut counts = self.ref_counts.write().await;
        *counts.entry(hash).or_insert(0) += 1;
    }

    /// Deregisters a reference to a hash.
    pub async fn deregister_ref(&self, hash: [u8; 32]) {
        let mut counts = self.ref_counts.write().await;
        if let Some(count) = counts.get_mut(&hash) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                counts.remove(&hash);
            }
        }
    }

    pub async fn put(&self, data: &[u8]) -> io::Result<[u8; 32]> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();

        let path = self.hash_to_path(&hash);
        if !path.exists() {
            let data_to_write = if let Some(ref enc) = self.encryption {
                enc.encrypt(data)?
            } else {
                data.to_vec()
            };

            let mut file = fs::File::create(&path).await?;
            file.write_all(&data_to_write).await?;
            file.sync_all().await?;
        }

        self.register_ref(hash).await;
        Ok(hash)
    }

    pub async fn get(&self, hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
        let path = self.hash_to_path(hash);
        if !path.exists() {
            return Ok(None);
        }

        let mut data = Vec::new();
        let mut file = fs::File::open(&path).await?;
        file.read_to_end(&mut data).await?;

        let decrypted_data = if let Some(ref enc) = self.encryption {
            enc.decrypt(&data)?
        } else {
            data
        };

        Ok(Some(decrypted_data))
    }

    /// Optimized GC: only removes files that have no active references in the ref_counts map.
    pub async fn gc_optimized(&self) -> io::Result<usize> {
        let mut deleted_count = 0;
        let mut read_dir = fs::read_dir(&self.root).await?;
        let active_hashes: HashSet<[u8; 32]> = {
            let counts = self.ref_counts.read().await;
            counts.keys().cloned().collect()
        };

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(hash_bytes) = hex::decode(file_name) {
                        if hash_bytes.len() == 32 {
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(&hash_bytes);
                            if !active_hashes.contains(&hash) {
                                fs::remove_file(&path).await?;
                                deleted_count += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(deleted_count)
    }

    /// Legacy GC for compatibility during transition.
    pub async fn gc(
        &self,
        active_hashes: &std::collections::HashSet<[u8; 32]>,
    ) -> io::Result<usize> {
        // Sync ref_counts with the provided set to ensure consistency
        {
            let mut counts = self.ref_counts.write().await;
            counts.clear();
            for &h in active_hashes {
                counts.insert(h, 1);
            }
        }
        self.gc_optimized().await
    }

    fn hash_to_path(&self, hash: &[u8; 32]) -> PathBuf {
        let hex = hex::encode(hash);
        self.root.join(hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_cas_roundtrip() {
        let dir = tempdir().unwrap();
        let cas = CASManager::new(dir.path(), None).unwrap();
        let data = b"content-addressable-data";

        let hash = cas.put(data).await.unwrap();
        let retrieved = cas.get(&hash).await.unwrap().unwrap();

        assert_eq!(data.to_vec(), retrieved);
    }

    #[tokio::test]
    async fn test_cas_deduplication() {
        let dir = tempdir().unwrap();
        let cas = CASManager::new(dir.path(), None).unwrap();
        let data = b"redundant-data";

        let hash1 = cas.put(data).await.unwrap();
        let hash2 = cas.put(data).await.unwrap();

        assert_eq!(hash1, hash2);

        let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(files.len(), 1);
        
        let counts = cas.ref_counts.read().await;
        assert_eq!(*counts.get(&hash1).unwrap(), 2);
    }

    #[tokio::test]
    async fn test_cas_ref_counting_gc() {
        let dir = tempdir().unwrap();
        let cas = CASManager::new(dir.path(), None).unwrap();

        let hash1 = cas.put(b"data1").await.unwrap();
        let _hash2 = cas.put(b"data2").await.unwrap();

        cas.deregister_ref(hash1).await; // count: 1 (one from put, one from deregister? wait, put registers 1)
        // Correct logic:
        // put registers 1. 
        // calling it again registers another 1.
        // So for _hash2, count is 1.
        // For hash1, count was 1, deregister makes it 0 (removes it).

        let deleted = cas.gc_optimized().await.unwrap();
        assert_eq!(deleted, 1); // data1 should be deleted

        assert!(cas.get(&hash1).await.unwrap().is_none());
        assert!(cas.get(&_hash2).await.unwrap().is_some());
    }
}
