use serde::{Serialize, Deserialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StorageTier {
    L0, // Hot - Raw Flushes
    L1, // Warm - Hourly
    L2, // Warm - Daily
    L3, // Cold - Archive
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    pub path: PathBuf,
    pub tier: StorageTier,
    pub window_start: u64, // Unix timestamp (nanos/secs)
    pub window_end: u64,
    pub size_bytes: u64,
}
