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
    pub window_start: u64, // Unix timestamp (nanos/secs)
    pub window_end: u64,
    pub size_bytes: u64,
}

pub struct CapacityManager {
    pub threshold: f32,
    pub hot_dir: PathBuf,
    pub cold_dir: PathBuf,
}

impl CapacityManager {
    pub fn find_eviction_candidates(&self, metadatas: &[SSTableMetadata]) -> Vec<SSTableMetadata> {
        let disks = Disks::new_with_refreshed_list();

        // Find the disk containing hot_dir
        let disk = disks
            .iter()
            .find(|d| self.hot_dir.starts_with(d.mount_point()));

        if let Some(d) = disk {
            let total = d.total_space();
            let available = d.available_space();
            let used = total - available;
            let usage_ratio = used as f32 / total as f32;

            if usage_ratio > self.threshold {
                let mut l2_metas: Vec<_> = metadatas
                    .iter()
                    .filter(|m| m.tier == StorageTier::L2)
                    .cloned()
                    .collect();

                // Sort by window_start (oldest first)
                l2_metas.sort_by_key(|m| m.window_start);

                let mut candidates = Vec::new();
                let mut projected_usage = usage_ratio;

                for meta in l2_metas {
                    if projected_usage <= self.threshold {
                        break;
                    }
                    candidates.push(meta.clone());
                    // Rough estimation of usage reduction
                    projected_usage -= meta.size_bytes as f32 / total as f32;
                }
                return candidates;
            }
        }

        vec![]
    }
}
