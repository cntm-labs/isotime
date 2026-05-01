use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use sysinfo::Disks;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
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
    pub window_start: u64,
    pub window_end: u64,
    pub size_bytes: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

pub struct CapacityManager {
    pub threshold: f32, // e.g., 0.85 (85%)
    pub hot_dir: PathBuf,
    pub cold_dir: PathBuf,
}

impl CapacityManager {
    /// Checks disk usage of the hot storage partition.
    pub fn get_hot_usage(&self) -> io::Result<f32> {
        let disks = Disks::new_with_refreshed_list();
        for disk in &disks {
            if self.hot_dir.starts_with(disk.mount_point()) {
                let total = disk.total_space();
                let available = disk.available_space();
                let used = total - available;
                return Ok(used as f32 / total as f32);
            }
        }
        Ok(0.0)
    }

    /// Selects candidates for eviction to cold storage based on capacity.
    pub fn find_eviction_candidates(&self, metadatas: &[SSTableMetadata]) -> Vec<SSTableMetadata> {
        let current_usage = self.get_hot_usage().unwrap_or(0.0);
        if current_usage < self.threshold {
            return vec![];
        }

        let mut sorted_metas = metadatas.to_vec();
        // Sort by tier descending, then by age (oldest first)
        sorted_metas.sort_by(|a, b| {
            b.tier
                .cmp(&a.tier)
                .then_with(|| a.window_start.cmp(&b.window_start))
        });

        let mut candidates = Vec::new();
        let mut projected_usage = current_usage;

        let disks = Disks::new_with_refreshed_list();
        let mut total = 1u64;
        for disk in &disks {
            if self.hot_dir.starts_with(disk.mount_point()) {
                total = disk.total_space();
                break;
            }
        }

        for meta in sorted_metas {
            if meta.tier != StorageTier::L3 {
                candidates.push(meta.clone());
                if total > 0 {
                    // Rough estimation of usage reduction
                    projected_usage -= meta.size_bytes as f32 / total as f32;
                }
                if projected_usage < self.threshold {
                    return candidates;
                }
            }
        }

        vec![]
    }
}

use std::io;
