use crate::storage::encryption::EncryptionManager;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, PartialEq, Eq)]
pub enum WalOp {
    Put(Vec<u8>, Vec<u8>, Vec<String>, Vec<(u32, u64)>),
    Delete(Vec<u8>),
}

#[derive(Serialize, Deserialize)]
struct WalEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    tags: Vec<String>,
}

enum WalRequest {
    Append(Vec<u8>, Vec<u8>, Vec<String>, Vec<(u32, u64)>, oneshot::Sender<io::Result<()>>),
    Delete(Vec<u8>, oneshot::Sender<io::Result<()>>),
}

pub struct Wal {
    tx: mpsc::Sender<WalRequest>,
}

impl Wal {
    pub async fn new<P: AsRef<Path>>(
        path: P,
        enc: Option<Arc<EncryptionManager>>,
    ) -> io::Result<(Arc<Self>, Vec<WalOp>)> {
        let path = path.as_ref().to_path_buf();
        let (tx, mut rx) = mpsc::channel::<WalRequest>(1024);

        // 1. Recover existing entries
        let mut entries = Vec::new();
        if path.exists() {
            let mut file = std::fs::File::open(&path)?;
            let mut buffer = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut buffer)?;

            if let Some(ref e) = enc {
                buffer = e.decrypt(&buffer)?;
            }

            let mut cursor = std::io::Cursor::new(buffer);
            while cursor.position() < cursor.get_ref().len() as u64 {
                let mut op_type = [0u8; 1];
                if std::io::Read::read_exact(&mut cursor, &mut op_type).is_err() {
                    break;
                }

                let mut len_buf = [0u8; 4];
                std::io::Read::read_exact(&mut cursor, &mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut payload = vec![0u8; len];
                std::io::Read::read_exact(&mut cursor, &mut payload)?;

                match op_type[0] {
                    1 => {
                        // Put: payload contains [key_len][key][val_len][val][tags_json][num_clock][clock...]
                        let mut payload_reader = std::io::Cursor::new(payload);
                        
                        let mut k_len_buf = [0u8; 4];
                        std::io::Read::read_exact(&mut payload_reader, &mut k_len_buf)?;
                        let k_len = u32::from_le_bytes(k_len_buf) as usize;
                        let mut key = vec![0u8; k_len];
                        std::io::Read::read_exact(&mut payload_reader, &mut key)?;

                        let mut v_len_buf = [0u8; 4];
                        std::io::Read::read_exact(&mut payload_reader, &mut v_len_buf)?;
                        let v_len = u32::from_le_bytes(v_len_buf) as usize;
                        let mut value = vec![0u8; v_len];
                        std::io::Read::read_exact(&mut payload_reader, &mut value)?;

                        let mut t_len_buf = [0u8; 4];
                        std::io::Read::read_exact(&mut payload_reader, &mut t_len_buf)?;
                        let t_len = u32::from_le_bytes(t_len_buf) as usize;
                        let mut tags_buf = vec![0u8; t_len];
                        std::io::Read::read_exact(&mut payload_reader, &mut tags_buf)?;
                        let tags: Vec<String> = serde_json::from_slice(&tags_buf)?;

                        // Read Vector Clock
                        let mut num_clock_buf = [0u8; 4];
                        std::io::Read::read_exact(&mut payload_reader, &mut num_clock_buf)?;
                        let num_clock = u32::from_le_bytes(num_clock_buf) as usize;
                        let mut clock = Vec::with_capacity(num_clock);
                        for _ in 0..num_clock {
                            let mut node_id_buf = [0u8; 4];
                            std::io::Read::read_exact(&mut payload_reader, &mut node_id_buf)?;
                            let node_id = u32::from_le_bytes(node_id_buf);
                            
                            let mut counter_buf = [0u8; 8];
                            std::io::Read::read_exact(&mut payload_reader, &mut counter_buf)?;
                            let counter = u64::from_le_bytes(counter_buf);
                            
                            clock.push((node_id, counter));
                        }

                        entries.push(WalOp::Put(key, value, tags, clock));
                    }
                    2 => {
                        entries.push(WalOp::Delete(payload));
                    }
                    _ => break,
                }
            }
        }

        // 2. Start worker thread for non-Send io_uring (if we were using it here)
        // For simplicity, we use standard tokio::fs in the background task.
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let res = match req {
                    WalRequest::Append(ref key, ref value, ref tags, ref clock, _) => {
                        let mut entry = Vec::new();
                        entry.push(1u8); // Type: Put

                        let tags_json = serde_json::to_vec(tags).unwrap();

                        // Layout: [key_len][key][val_len][val][tags_len][tags_json][num_clock][clock...]
                        let mut payload = Vec::new();
                        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
                        payload.extend_from_slice(key);
                        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        payload.extend_from_slice(value);
                        payload.extend_from_slice(&(tags_json.len() as u32).to_le_bytes());
                        payload.extend_from_slice(&tags_json);

                        // Vector Clock serialization
                        payload.extend_from_slice(&(clock.len() as u32).to_le_bytes());
                        for (node_id, counter) in clock {
                            payload.extend_from_slice(&node_id.to_le_bytes());
                            payload.extend_from_slice(&counter.to_le_bytes());
                        }

                        entry.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                        entry.extend_from_slice(&payload);

                        Self::write_to_disk(&path, entry, enc.as_deref()).await
                    }
                    WalRequest::Delete(ref key, _) => {
                        let mut entry = Vec::new();
                        entry.push(2u8); // Type: Delete
                        entry.extend_from_slice(&(key.len() as u32).to_le_bytes());
                        entry.extend_from_slice(key);

                        Self::write_to_disk(&path, entry, enc.as_deref()).await
                    }
                };

                // Send reply
                match req {
                    WalRequest::Append(.., reply) => {
                        let _ = reply.send(res);
                    }
                    WalRequest::Delete(.., reply) => {
                        let _ = reply.send(res);
                    }
                }
            }
        });

        Ok((Arc::new(Self { tx }), entries))
    }

    async fn write_to_disk(
        path: &Path,
        data: Vec<u8>,
        enc: Option<&EncryptionManager>,
    ) -> io::Result<()> {
        // In a real io_uring implementation, we'd use tokio-uring here.
        // For WAL simplicity and thread-safety across the app, we use O_APPEND style writes.
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        let final_data = if let Some(e) = enc {
            e.encrypt(&data)?
        } else {
            data
        };

        file.write_all(&final_data).await?;
        file.flush().await?;
        Ok(())
    }

    pub async fn append(
        &self,
        key: &[u8],
        value: &[u8],
        tags: Vec<String>,
        clock: Vec<(u32, u64)>,
    ) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WalRequest::Append(
                key.to_vec(),
                value.to_vec(),
                tags,
                clock,
                tx,
            ))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL worker died"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL reply dropped"))?
    }

    pub async fn delete(&self, key: &[u8]) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WalRequest::Delete(key.to_vec(), tx))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL worker died"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "WAL reply dropped"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_wal_append_recover() {
        let path = "test.wal";
        if Path::new(path).exists() {
            fs::remove_file(path).unwrap();
        }

        {
            let (wal, _) = Wal::new(path, None).await.unwrap();
            wal.append(b"key1", b"value1", vec!["tag1".to_string()], vec![])
                .await
                .unwrap();
            wal.append(b"key2", b"value2", vec![], vec![])
                .await
                .unwrap();
        }

        {
            let (_, entries) = Wal::new(path, None).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(
                entries[0],
                WalOp::Put(
                    b"key1".to_vec(),
                    b"value1".to_vec(),
                    vec!["tag1".to_string()],
                    vec![]
                )
            );
            assert_eq!(
                entries[1],
                WalOp::Put(b"key2".to_vec(), b"value2".to_vec(), vec![], vec![])
            );
        }

        fs::remove_file(path).unwrap();
    }
}
