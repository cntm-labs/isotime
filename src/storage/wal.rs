use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub enum WalOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[0])?;
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
        self.writer.write_all(value)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[1])?;
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key)?;
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
            let mut op_type = [0u8; 1];
            match reader.read_exact(&mut op_type) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            match op_type[0] {
                0 => {
                    let mut val_len_buf = [0u8; 4];
                    reader.read_exact(&mut val_len_buf)?;
                    let val_len = u32::from_le_bytes(val_len_buf) as usize;
                    let mut value = vec![0u8; val_len];
                    reader.read_exact(&mut value)?;
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
            let mut wal = WAL::new(wal_path).unwrap();
            wal.append(b"key1", b"value1").unwrap();
            wal.append(b"key2", b"value2").unwrap();
            wal.delete(b"key1").unwrap();
        }

        {
            let wal = WAL::new(wal_path).unwrap();
            let entries = wal.recover().unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], WalOp::Put(b"key1".to_vec(), b"value1".to_vec()));
            assert_eq!(entries[1], WalOp::Put(b"key2".to_vec(), b"value2".to_vec()));
            assert_eq!(entries[2], WalOp::Delete(b"key1".to_vec()));
        }

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn test_wal_empty_recover() {
        let wal_path = "test_empty.wal";
        if Path::new(wal_path).exists() {
            fs::remove_file(wal_path).unwrap();
        }

        let wal = WAL::new(wal_path).unwrap();
        let entries = wal.recover().unwrap();
        assert!(entries.is_empty());

        fs::remove_file(wal_path).unwrap();
    }
}
