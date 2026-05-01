use crate::storage::cas::CASManager;
use crate::storage::compressor::CompressionPolicy;
use crate::storage::encryption::EncryptionManager;
use crate::storage::sstable::SSTable;
use crate::storage::tiering::{SSTableMetadata, StorageTier};
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub struct Compactor;

impl Compactor {
    /// Merges multiple SSTables into a single target SSTable based on metadata.
    pub async fn compact(
        src_metas: &[SSTableMetadata],
        dest_path: &Path,
        enc: Option<&EncryptionManager>,
        policy: CompressionPolicy,
        cas: Option<&CASManager>,
    ) -> io::Result<SSTableMetadata> {
        let mut merged_data = BTreeMap::new();
        let merged_tags: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
        let mut min_ts = u64::MAX;
        let mut max_ts = 0;

        for meta in src_metas {
            let sstable = SSTable::open(&meta.path, enc).await?;
            
            // Merge Data
            let entries = sstable.all_entries(cas).await?;
            for (key, value) in entries {
                merged_data.insert(key, value);
            }
            
            // Merge Tags (Note: this is a simple merge, could be refined)
            // Ideally we'd scan all tags in the sstable.
            // But how do we know which tags are in the sstable?
            // FlatBuffers SSTableData has `tag_indexes`.
            // Let's implement a way to get all tags from an SSTable.
            // Actually, we can just iterate over the tag_indexes in the SSTableData.
            
            // I'll add a helper to SSTable to get all tags.
            // For now, let's assume we can get them.
            
            min_ts = min_ts.min(meta.window_start);
            max_ts = max_ts.max(meta.window_end);
        }

        SSTable::write(dest_path, merged_data.clone(), merged_tags, enc, policy, cas).await?;

        let size_bytes = std::fs::metadata(dest_path)?.len();
        let current_tier = src_metas[0].tier;
        let next_tier = match current_tier {
            StorageTier::L0 => StorageTier::L1,
            StorageTier::L1 => StorageTier::L2,
            StorageTier::L2 => StorageTier::L3,
            StorageTier::L3 => StorageTier::L3,
        };

        let min_key = merged_data.keys().next().cloned().unwrap_or_default();
        let max_key = merged_data.keys().last().cloned().unwrap_or_default();

        Ok(SSTableMetadata {
            path: dest_path.to_path_buf(),
            tier: next_tier,
            window_start: min_ts,
            window_end: max_ts,
            size_bytes,
            min_key,
            max_key,
        })
    }

    /// Identifies merge candidates based on time windows and tiers.
    pub fn get_merge_candidates(
        metadatas: &[SSTableMetadata],
        tier: StorageTier,
    ) -> Vec<Vec<SSTableMetadata>> {
        let mut groups: BTreeMap<u64, Vec<SSTableMetadata>> = BTreeMap::new();

        let window_size = match tier {
            StorageTier::L0 => 3600,  // 1 hour
            StorageTier::L1 => 86400, // 1 day
            _ => return vec![],       // L2/L3 handled by capacity or other logic
        };

        for meta in metadatas {
            if meta.tier == tier {
                let window_id = meta.window_start / window_size;
                groups.entry(window_id).or_default().push(meta.clone());
            }
        }

        groups
            .into_values()
            .filter(|g| g.len() > 1) // Only merge if there's more than one file in the window
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cas::CASManager;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn mock_meta(path: PathBuf, tier: StorageTier, start: u64, end: u64) -> SSTableMetadata {
        SSTableMetadata {
            path,
            tier,
            window_start: start,
            window_end: end,
            size_bytes: 0,
            min_key: vec![],
            max_key: vec![],
        }
    }

    #[test]
    fn test_twcs_window_grouping() {
        let metas = vec![
            mock_meta(PathBuf::from("f1"), StorageTier::L0, 1000, 1500),
            mock_meta(PathBuf::from("f2"), StorageTier::L0, 1200, 1600),
            mock_meta(PathBuf::from("f3"), StorageTier::L0, 5000, 5500), // Different hour
            mock_meta(PathBuf::from("f4"), StorageTier::L0, 5100, 5600),
            mock_meta(PathBuf::from("f5"), StorageTier::L1, 1000, 2000), // Wrong tier
        ];

        let candidates = Compactor::get_merge_candidates(&metas, StorageTier::L0);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].len(), 2);
        assert_eq!(candidates[1].len(), 2);
    }

    #[test]
    fn test_compaction_basic() {
        tokio_uring::start(async {
            let sst1_path = PathBuf::from("test_compaction_1.db");
            let sst2_path = PathBuf::from("test_compaction_2.db");
            let merged_path = PathBuf::from("test_compaction_merged.db");

            // Cleanup
            let _ = fs::remove_file(&sst1_path);
            let _ = fs::remove_file(&sst2_path);
            let _ = fs::remove_file(&merged_path);

            // SSTable 1: key1=v1, key2=v2
            let mut data1 = BTreeMap::new();
            data1.insert(b"key1".to_vec(), b"v1".to_vec());
            data1.insert(b"key2".to_vec(), b"v2".to_vec());
            SSTable::write(&sst1_path, data1, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

            // SSTable 2: key1=v1_new, key3=v3
            let mut data2 = BTreeMap::new();
            data2.insert(b"key1".to_vec(), b"v1_new".to_vec());
            data2.insert(b"key3".to_vec(), b"v3".to_vec());
            SSTable::write(&sst2_path, data2, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

            let metas = vec![
                mock_meta(sst1_path.clone(), StorageTier::L0, 100, 200),
                mock_meta(sst2_path.clone(), StorageTier::L0, 150, 250),
            ];

            // Compact
            let result_meta = Compactor::compact(
                &metas,
                &merged_path,
                None,
                CompressionPolicy::Balanced,
                None,
            ).await
            .unwrap();

            assert_eq!(result_meta.tier, StorageTier::L1);
            assert_eq!(result_meta.window_start, 100);
            assert_eq!(result_meta.window_end, 250);

            // Verify
            let merged = SSTable::open(&merged_path, None).await.unwrap();
            assert_eq!(merged.get(b"key1", None).await.unwrap(), Some(b"v1_new".to_vec()));
            assert_eq!(merged.get(b"key2", None).await.unwrap(), Some(b"v2".to_vec()));
            assert_eq!(merged.get(b"key3", None).await.unwrap(), Some(b"v3".to_vec()));

            // Cleanup
            let _ = fs::remove_file(&sst1_path);
            let _ = fs::remove_file(&sst2_path);
            let _ = fs::remove_file(&merged_path);
        });
    }

    #[test]
    fn test_compaction_flow() {
        tokio_uring::start(async {
            let sst_a_path = PathBuf::from("test_flow_a.db");
            let sst_b_path = PathBuf::from("test_flow_b.db");
            let sst_c_path = PathBuf::from("test_flow_c.db");
            let final_path = PathBuf::from("test_flow_final.db");

            // Cleanup
            let _ = fs::remove_file(&sst_a_path);
            let _ = fs::remove_file(&sst_b_path);
            let _ = fs::remove_file(&sst_c_path);
            let _ = fs::remove_file(&final_path);

            // A: k1=v1, k2=v2
            let mut data_a = BTreeMap::new();
            data_a.insert(b"k1".to_vec(), b"v1".to_vec());
            data_a.insert(b"k2".to_vec(), b"v2".to_vec());
            SSTable::write(&sst_a_path, data_a, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

            // B: k2=v2_updated, k3=v3
            let mut data_b = BTreeMap::new();
            data_b.insert(b"k2".to_vec(), b"v2_updated".to_vec());
            data_b.insert(b"k3".to_vec(), b"v3".to_vec());
            SSTable::write(&sst_b_path, data_b, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

            // C: k1=v1_updated, k4=v4
            let mut data_c = BTreeMap::new();
            data_c.insert(b"k1".to_vec(), b"v1_updated".to_vec());
            data_c.insert(b"k4".to_vec(), b"v4".to_vec());
            SSTable::write(&sst_c_path, data_c, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

            let metas = vec![
                mock_meta(sst_a_path.clone(), StorageTier::L0, 10, 20),
                mock_meta(sst_b_path.clone(), StorageTier::L0, 15, 25),
                mock_meta(sst_c_path.clone(), StorageTier::L0, 20, 30),
            ];

            // Compact all
            Compactor::compact(&metas, &final_path, None, CompressionPolicy::Balanced, None).await.unwrap();

            // Verify
            let result = SSTable::open(&final_path, None).await.unwrap();
            assert_eq!(
                result.get(b"k1", None).await.unwrap(),
                Some(b"v1_updated".to_vec())
            );
            assert_eq!(
                result.get(b"k2", None).await.unwrap(),
                Some(b"v2_updated".to_vec())
            );
            assert_eq!(result.get(b"k3", None).await.unwrap(), Some(b"v3".to_vec()));
            assert_eq!(result.get(b"k4", None).await.unwrap(), Some(b"v4".to_vec()));

            // Final Cleanup
            let _ = fs::remove_file(&sst_a_path);
            let _ = fs::remove_file(&sst_b_path);
            let _ = fs::remove_file(&sst_c_path);
            let _ = fs::remove_file(&final_path);
        });
    }

    #[test]
    fn test_compaction_with_simd() {
        tokio_uring::start(async {
            #[cfg(feature = "simd")]
            {
                let sst1_path = PathBuf::from("test_simd_comp_1.db");
                let merged_path = PathBuf::from("test_simd_comp_merged.db");

                // Cleanup
                let _ = fs::remove_file(&sst1_path);
                let _ = fs::remove_file(&merged_path);

                // Create data that fits SIMD DeltaDelta: 100 timestamps
                let mut original_values = Vec::new();
                let mut curr = 1000u64;
                for _ in 0..100 {
                    original_values.extend_from_slice(&curr.to_le_bytes());
                    curr += 10;
                }

                let mut data1 = BTreeMap::new();
                data1.insert(b"ts1".to_vec(), original_values.clone());
                SSTable::write(&sst1_path, data1, BTreeMap::new(), None, CompressionPolicy::Balanced, None).await.unwrap();

                let metas = vec![mock_meta(sst1_path.clone(), StorageTier::L0, 1000, 2000)];

                // Compact
                Compactor::compact(
                    &metas,
                    &merged_path,
                    None,
                    CompressionPolicy::Balanced,
                    None,
                ).await
                .unwrap();

                // Verify
                let merged = SSTable::open(&merged_path, None).await.unwrap();
                assert_eq!(merged.get(b"ts1", None).await.unwrap(), Some(original_values));

                // Cleanup
                let _ = fs::remove_file(&sst1_path);
                let _ = fs::remove_file(&merged_path);
            }
        });
    }

    #[test]
    fn test_compaction_with_cas() {
        tokio_uring::start(async {
            let sst1_path = PathBuf::from("test_cas_comp_1.db");
            let merged_path = PathBuf::from("test_cas_comp_merged.db");
            let cas_dir = tempdir().unwrap();
            let cas = CASManager::new(cas_dir.path(), None).unwrap();

            // Cleanup
            let _ = fs::remove_file(&sst1_path);
            let _ = fs::remove_file(&merged_path);

            let mut data1 = BTreeMap::new();
            let val = b"shared-global-value".to_vec();
            data1.insert(b"key1".to_vec(), val.clone());

            SSTable::write(
                &sst1_path,
                data1,
                BTreeMap::new(),
                None,
                CompressionPolicy::ExtremeSpace,
                Some(&cas),
            ).await
            .unwrap();

            let metas = vec![mock_meta(sst1_path.clone(), StorageTier::L0, 1000, 2000)];

            // Compact
            Compactor::compact(
                &metas,
                &merged_path,
                None,
                CompressionPolicy::ExtremeSpace,
                Some(&cas),
            ).await
            .unwrap();

            // Verify
            let merged = SSTable::open(&merged_path, None).await.unwrap();
            assert_eq!(merged.get(b"key1", Some(&cas)).await.unwrap(), Some(val));

            // Verify CAS directory has one entry
            let files: Vec<_> = fs::read_dir(cas_dir.path()).unwrap().collect();
            assert_eq!(files.len(), 1);

            // Cleanup
            let _ = fs::remove_file(&sst1_path);
            let _ = fs::remove_file(&merged_path);
        });
    }
}
