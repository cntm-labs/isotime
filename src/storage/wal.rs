use crate::storage::encryption::EncryptionManager;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_uring::fs::File;

#[derive(Debug, PartialEq, Eq)]
pub enum WalOp {
    Put(Vec<u8>, Vec<u8>, Vec<String>),
    Delete(Vec<u8>),
}

enum WalRequest {
    Append(Vec<u8>, Vec<u8>, Vec<String>, oneshot::Sender<io::Result<()>>),
    Delete(Vec<u8>, oneshot::Sender<io::Result<()>>),
}

/// A handle to the Write-Ahead Log.
/// Uses a background thread with io_uring for high-performance logging.
#[derive(Clone)]
pub struct Wal {
    tx: mpsc::Sender<WalRequest>,
}

impl Wal {
    pub async fn new<P: AsRef<Path>>(
        path: P,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> io::Result<(Self, Vec<WalOp>)> {
        let path_buf = path.as_ref().to_path_buf();
        let (tx, mut rx) = mpsc::channel(1024);

        // Recovery is done synchronously or via a temporary task before starting the worker
        let recovered_entries = Self::do_recovery(&path_buf, encryption_manager.clone()).await?;

        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let file = if path_buf.exists() {
                    File::open(&path_buf).await.expect("Failed to open WAL")
                } else {
                    File::create(&path_buf).await.expect("Failed to create WAL")
                };

                let metadata = std::fs::metadata(&path_buf).expect("Failed to get WAL metadata");
                let mut offset = metadata.len();

                while let Some(req) = rx.recv().await {
                    match req {
                        WalRequest::Append(key, value, tags, reply) => {
                            let mut entry = Vec::new();
                            entry.push(0u8);
                            entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
                            entry.extend_from_slice(&key);
                            entry.extend_from_slice(&(value.len() as u32).to_le_bytes());
                            entry.extend_from_slice(&value);

                            // Tags serialization
                            entry.extend_from_slice(&(tags.len() as u32).to_le_bytes());
                            for tag in tags {
                                entry.extend_from_slice(&(tag.len() as u32).to_le_bytes());
                                entry.extend_from_slice(tag.as_bytes());
                            }

                            let res = Self::write_entry(
                                &file,
                                &mut offset,
                                entry,
                                encryption_manager.as_deref(),
                            )
                            .await;
                            let _ = reply.send(res);
                        }
                        WalRequest::Delete(key, reply) => {
                            let mut entry = Vec::new();
                            entry.push(1u8);
                            entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
                            entry.extend_from_slice(&key);

                            let res = Self::write_entry(
                                &file,
                                &mut offset,
                                entry,
                                encryption_manager.as_deref(),
                            )
                            .await;
                            let _ = reply.send(res);
                        }
                    }
                }
            });
        });

        Ok((Self { tx }, recovered_entries))
    }

    async fn write_entry(
        file: &File,
        offset: &mut u64,
        entry: Vec<u8>,
        enc: Option<&EncryptionManager>,
    ) -> io::Result<()> {
        let final_payload = if let Some(em) = enc {
            em.encrypt(&entry)?
        } else {
            entry
        };

        let mut buf = Vec::with_capacity(4 + final_payload.len());
        buf.extend_from_slice(&(final_payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&final_payload);

        let (res, _) = file.write_at(buf, *offset).await;
        let n = res?;
        *offset += n as u64;
        file.sync_data().await?;
        Ok(())
    }

    async fn do_recovery(
        path: &PathBuf,
        enc: Option<Arc<EncryptionManager>>,
    ) -> io::Result<Vec<WalOp>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        // Recovery can use standard tokio::fs since it's not the hot path
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(path).await?;
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; payload_len];
            file.read_exact(&mut payload).await?;

            let decrypted_payload = if let Some(ref em) = enc {
                em.decrypt(&payload)?
            } else {
                payload
            };

            let mut payload_reader = &decrypted_payload[..];
            let mut op_type = [0u8; 1];
            std::io::Read::read_exact(&mut payload_reader, &mut op_type)?;

            let mut key_len_buf = [0u8; 4];
            std::io::Read::read_exact(&mut payload_reader, &mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key = vec![0u8; key_len];
            std::io::Read::read_exact(&mut payload_reader, &mut key)?;

            match op_type[0] {
                0 => {
                    let mut val_len_buf = [0u8; 4];
                    std::io::Read::read_exact(&mut payload_reader, &mut val_len_buf)?;
                    let val_len = u32::from_le_bytes(val_len_buf) as usize;
                    let mut value = vec![0u8; val_len];
                    std::io::Read::read_exact(&mut payload_reader, &mut value)?;

                    // Read tags
                    let mut num_tags_buf = [0u8; 4];
                    std::io::Read::read_exact(&mut payload_reader, &mut num_tags_buf)?;
                    let num_tags = u32::from_le_bytes(num_tags_buf) as usize;
                    let mut tags = Vec::with_capacity(num_tags);
                    for _ in 0..num_tags {
                        let mut tag_len_buf = [0u8; 4];
                        std::io::Read::read_exact(&mut payload_reader, &mut tag_len_buf)?;
                        let tag_len = u32::from_le_bytes(tag_len_buf) as usize;
                        let mut tag_bytes = vec![0u8; tag_len];
                        std::io::Read::read_exact(&mut payload_reader, &mut tag_bytes)?;
                        tags.push(String::from_utf8_lossy(&tag_bytes).to_string());
                    }

                    entries.push(WalOp::Put(key, value, tags));
                }
                1 => {
                    entries.push(WalOp::Delete(key));
                }
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid WAL op")),
            }
        }
        Ok(entries)
    }

    pub async fn append(&self, key: &[u8], value: &[u8], tags: Vec<String>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WalRequest::Append(key.to_vec(), value.to_vec(), tags, tx))
            .await
            .map_err(|_| io::Error::other("WAL worker died"))?;
        rx.await
            .map_err(|_| io::Error::other("WAL worker dropped request"))?
    }

    pub async fn delete(&self, key: &[u8]) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WalRequest::Delete(key.to_vec(), tx))
            .await
            .map_err(|_| io::Error::other("WAL worker died"))?;
        rx.await
            .map_err(|_| io::Error::other("WAL worker dropped request"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_wal_append_recover() {
        let wal_path = "test_append.wal";
        if Path::new(wal_path).exists() {
            let _ = fs::remove_file(wal_path);
        }

        {
            let (wal, _) = Wal::new(wal_path, None).await.unwrap();
            wal.append(b"key1", b"value1", vec!["tag1".to_string()]).await.unwrap();
            wal.append(b"key2", b"value2", vec![]).await.unwrap();
            wal.delete(b"key1").await.unwrap();
            // Drop wal triggers sender drop
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        {
            let (_wal, entries) = Wal::new(wal_path, None).await.unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], WalOp::Put(b"key1".to_vec(), b"value1".to_vec(), vec!["tag1".to_string()]));
            assert_eq!(entries[1], WalOp::Put(b"key2".to_vec(), b"value2".to_vec(), vec![]));
            assert_eq!(entries[2], WalOp::Delete(b"key1".to_vec()));
        }

        let _ = fs::remove_file(wal_path);
    }
}
