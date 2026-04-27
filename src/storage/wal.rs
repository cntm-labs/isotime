use crate::storage::encryption::EncryptionManager;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub enum WalOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl Wal {
    pub fn new<P: AsRef<Path>>(path: P, encryption_manager: Option<Arc<EncryptionManager>>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: BufWriter::new(file),
            encryption_manager,
        })
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut entry = Vec::new();
        entry.push(0u8); // OpType::Put
        entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry.extend_from_slice(key);
        entry.extend_from_slice(&(value.len() as u32).to_le_bytes());
        entry.extend_from_slice(value);

        self.write_entry(&entry)
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let mut entry = Vec::new();
        entry.push(1u8); // OpType::Delete
        entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry.extend_from_slice(key);

        self.write_entry(&entry)
    }

    fn write_entry(&mut self, entry: &[u8]) -> io::Result<()> {
        let final_payload = if let Some(ref em) = self.encryption_manager {
            em.encrypt(entry)?
        } else {
            entry.to_vec()
        };

        self.writer.write_all(&(final_payload.len() as u32).to_le_bytes())?;
        self.writer.write_all(&final_payload)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn recover(&self) -> io::Result<Vec<WalOp>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; payload_len];
            reader.read_exact(&mut payload)?;

            let decrypted_payload = if let Some(ref em) = self.encryption_manager {
                em.decrypt(&payload)?
            } else {
                payload
            };

            let mut payload_reader = &decrypted_payload[..];
            let mut op_type = [0u8; 1];
            payload_reader.read_exact(&mut op_type)?;

            let mut key_len_buf = [0u8; 4];
            payload_reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key = vec![0u8; key_len];
            payload_reader.read_exact(&mut key)?;

            match op_type[0] {
                0 => {
                    let mut val_len_buf = [0u8; 4];
                    payload_reader.read_exact(&mut val_len_buf)?;
                    let val_len = u32::from_le_bytes(val_len_buf) as usize;
                    let mut value = vec![0u8; val_len];
                    payload_reader.read_exact(&mut value)?;
                    entries.push(WalOp::Put(key, value));
                }
                1 => {
                    entries.push(WalOp::Delete(key));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid WAL op type",
                    ))
                }
            }
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_wal_append_recover() {
        let wal_path = "test_append.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        {
            let mut wal = Wal::new(wal_path, None).unwrap();
            wal.append(b"key1", b"value1").unwrap();
            wal.append(b"key2", b"value2").unwrap();
            wal.delete(b"key1").unwrap();
        }

        {
            let wal = Wal::new(wal_path, None).unwrap();
            let entries = wal.recover().unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], WalOp::Put(b"key1".to_vec(), b"value1".to_vec()));
            assert_eq!(entries[1], WalOp::Put(b"key2".to_vec(), b"value2".to_vec()));
            assert_eq!(entries[2], WalOp::Delete(b"key1".to_vec()));
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_wal_encrypted_append_recover() {
        let wal_path = "test_encrypted_append.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let key = [0u8; 32];
        let em = Arc::new(EncryptionManager::new(&key));

        {
            let mut wal = Wal::new(wal_path, Some(em.clone())).unwrap();
            wal.append(b"key1", b"value1").unwrap();
            wal.append(b"key2", b"value2").unwrap();
        }

        // Verify that the file content is actually encrypted (not just plaintext)
        let file_content = fs::read(wal_path).unwrap();
        assert!(!file_content.windows(4).any(|w| w == b"key1"));

        {
            let wal = Wal::new(wal_path, Some(em)).unwrap();
            let entries = wal.recover().unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0], WalOp::Put(b"key1".to_vec(), b"value1".to_vec()));
            assert_eq!(entries[1], WalOp::Put(b"key2".to_vec(), b"value2".to_vec()));
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_wal_empty_recover() {
        let wal_path = "test_empty.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let wal = Wal::new(wal_path, None).unwrap();
        let entries = wal.recover().unwrap();
        assert!(entries.is_empty());

        fs::remove_file(wal_path).unwrap();
    }
}
