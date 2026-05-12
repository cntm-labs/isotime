use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot};

pub enum IoRequest {
    Read {
        path: PathBuf,
        offset: u64,
        len: usize,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Write {
        path: PathBuf,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<io::Result<()>>,
    },
}

pub struct IoPool {
    tx: mpsc::Sender<IoRequest>,
}

impl IoPool {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel(capacity);

        thread::spawn(move || {
            tokio_uring::start(async move {
                while let Some(req) = rx.recv().await {
                    match req {
                        IoRequest::Read { path, offset, len, reply } => {
                            let res = Self::handle_read(path, offset, len).await;
                            let _ = reply.send(res);
                        }
                        IoRequest::Write { path, offset, data, reply } => {
                            let res = Self::handle_write(path, offset, data).await;
                            let _ = reply.send(res);
                        }
                    }
                }
            });
        });

        Arc::new(Self { tx })
    }

    async fn handle_read(path: PathBuf, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let file = tokio_uring::fs::File::open(path).await?;
        let buf = vec![0u8; len];
        let (res, buf) = file.read_at(buf, offset).await;
        res?;
        Ok(buf)
    }

    async fn handle_write(path: PathBuf, offset: u64, data: Vec<u8>) -> io::Result<()> {
        let file = tokio_uring::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .await?;
        let (res, _) = file.write_at(data, offset).await;
        res?;
        Ok(())
    }

    pub async fn read(&self, path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Read {
                path: path.to_path_buf(),
                offset,
                len,
                reply: tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "IO Pool worker died"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "IO Pool reply dropped"))?
    }

    pub async fn write(&self, path: &Path, offset: u64, data: Vec<u8>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Write {
                path: path.to_path_buf(),
                offset,
                data,
                reply: tx,
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "IO Pool worker died"))?;
        rx.await.map_err(|_| io::Error::new(io::ErrorKind::Other, "IO Pool reply dropped"))?
    }
}
