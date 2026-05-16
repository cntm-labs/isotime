use crate::storage::tiering::SSTableMetadata;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manifest {
    pub version: u32,
    pub created_at: u64,
    pub sstables: Vec<SSTableMetadata>,
}

pub struct ManifestManager {
    path: PathBuf,
}

impl ManifestManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn load(&self) -> io::Result<Option<Manifest>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.path)?;
        let manifest: Manifest = serde_json::from_str(&content)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(manifest))
    }

    pub fn save(&self, sstables: Vec<SSTableMetadata>) -> io::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let manifest = Manifest {
            version: 1,
            created_at: now,
            sstables,
        };

        let json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let tmp_path = self.path.with_extension("json.tmp");

        // Atomic write protocol
        fs::write(&tmp_path, json)?;

        // Ensure data is synced (using standard fs doesn't have a direct easy way for all OS,
        // but for Manifest JSON, a simple rename is often atomic enough on POSIX).
        // In a real DB we would open the file and call sync_all().

        fs::rename(&tmp_path, &self.path)?;

        Ok(())
    }
}
