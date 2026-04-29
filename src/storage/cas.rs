use crate::storage::encryption::EncryptionManager;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct CASManager {
    root: PathBuf,
    encryption: Option<Arc<EncryptionManager>>,
}

impl CASManager {
    pub fn new<P: AsRef<Path>>(
        root: P,
        encryption: Option<Arc<EncryptionManager>>,
    ) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        if !root.exists() {
            fs::create_dir_all(&root)?;
        }
        Ok(Self { root, encryption })
    }

    pub fn put(&self, data: &[u8]) -> io::Result<[u8; 32]> {
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

            let mut file = fs::File::create(path)?;
            file.write_all(&data_to_write)?;
        }

        Ok(hash)
    }

    pub fn get(&self, hash: &[u8; 32]) -> io::Result<Option<Vec<u8>>> {
        let path = self.hash_to_path(hash);
        if !path.exists() {
            return Ok(None);
        }

        let mut data = Vec::new();
        let mut file = fs::File::open(path)?;
        file.read_to_end(&mut data)?;

        let decrypted_data = if let Some(ref enc) = self.encryption {
            enc.decrypt(&data)?
        } else {
            data
        };

        Ok(Some(decrypted_data))
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

    #[test]
    fn test_cas_roundtrip() {
        let dir = tempdir().unwrap();
        let cas = CASManager::new(dir.path(), None).unwrap();
        let data = b"content-addressable-data";

        let hash = cas.put(data).unwrap();
        let retrieved = cas.get(&hash).unwrap().unwrap();

        assert_eq!(data.to_vec(), retrieved);
    }

    #[test]
    fn test_cas_deduplication() {
        let dir = tempdir().unwrap();
        let cas = CASManager::new(dir.path(), None).unwrap();
        let data = b"redundant-data";

        let hash1 = cas.put(data).unwrap();
        let hash2 = cas.put(data).unwrap();

        assert_eq!(hash1, hash2);

        let files: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_cas_encrypted() {
        let dir = tempdir().unwrap();
        let key = [0u8; 32];
        let enc = Arc::new(EncryptionManager::new(&key));
        let cas = CASManager::new(dir.path(), Some(enc)).unwrap();

        let data = b"secret-cas-data";
        let hash = cas.put(data).unwrap();

        // Read raw file to verify it's encrypted
        let path = cas.hash_to_path(&hash);
        let mut raw_data = Vec::new();
        let mut file = fs::File::open(path).unwrap();
        file.read_to_end(&mut raw_data).unwrap();
        assert_ne!(raw_data, data.to_vec()); // Should be ciphertext

        // Decrypt via CAS manager
        let retrieved = cas.get(&hash).unwrap().unwrap();
        assert_eq!(data.to_vec(), retrieved);
    }
}
