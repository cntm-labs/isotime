use crate::storage::compressor::CompressionPolicy;
use crate::storage::encryption::EncryptionManager;
use crate::storage::sstable::SSTable;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

pub struct Compactor;

impl Compactor {
    /// Merges multiple SSTables into a single target SSTable.
    /// SSTables should be provided in chronological order (oldest to newest).
    pub fn compact(
        src_paths: &[&Path],
        dest_path: &Path,
        enc: Option<&EncryptionManager>,
        policy: CompressionPolicy,
    ) -> io::Result<()> {
        let mut merged_data = BTreeMap::new();

        for path in src_paths {
            let sstable = SSTable::open(path, enc)?;
            let entries = sstable.all_entries()?;
            for (key, value) in entries {
                merged_data.insert(key, value);
            }
        }

        SSTable::write(dest_path, merged_data, enc, policy)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_compaction_basic() {
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
        SSTable::write(&sst1_path, data1, None, CompressionPolicy::Balanced).unwrap();

        // SSTable 2: key1=v1_new, key3=v3
        let mut data2 = BTreeMap::new();
        data2.insert(b"key1".to_vec(), b"v1_new".to_vec());
        data2.insert(b"key3".to_vec(), b"v3".to_vec());
        SSTable::write(&sst2_path, data2, None, CompressionPolicy::Balanced).unwrap();

        // Compact
        Compactor::compact(
            &[&sst1_path, &sst2_path],
            &merged_path,
            None,
            CompressionPolicy::Balanced,
        )
        .unwrap();

        // Verify
        let merged = SSTable::open(&merged_path, None).unwrap();
        assert_eq!(merged.get(b"key1").unwrap(), Some(b"v1_new".to_vec()));
        assert_eq!(merged.get(b"key2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(merged.get(b"key3").unwrap(), Some(b"v3".to_vec()));

        // Cleanup
        fs::remove_file(&sst1_path).unwrap();
        fs::remove_file(&sst2_path).unwrap();
        fs::remove_file(&merged_path).unwrap();
    }

    #[tokio::test]
    async fn test_compaction_flow() {
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
        SSTable::write(&sst_a_path, data_a, None, CompressionPolicy::Balanced).unwrap();

        // B: k2=v2_updated, k3=v3
        let mut data_b = BTreeMap::new();
        data_b.insert(b"k2".to_vec(), b"v2_updated".to_vec());
        data_b.insert(b"k3".to_vec(), b"v3".to_vec());
        SSTable::write(&sst_b_path, data_b, None, CompressionPolicy::Balanced).unwrap();

        // C: k1=v1_updated, k4=v4
        let mut data_c = BTreeMap::new();
        data_c.insert(b"k1".to_vec(), b"v1_updated".to_vec());
        data_c.insert(b"k4".to_vec(), b"v4".to_vec());
        SSTable::write(&sst_c_path, data_c, None, CompressionPolicy::Balanced).unwrap();

        // Compact all
        Compactor::compact(
            &[&sst_a_path, &sst_b_path, &sst_c_path],
            &final_path,
            None,
            CompressionPolicy::Balanced,
        )
        .unwrap();

        // Verify
        let result = SSTable::open(&final_path, None).unwrap();
        assert_eq!(result.get(b"k1").unwrap(), Some(b"v1_updated".to_vec()));
        assert_eq!(result.get(b"k2").unwrap(), Some(b"v2_updated".to_vec()));
        assert_eq!(result.get(b"k3").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(result.get(b"k4").unwrap(), Some(b"v4".to_vec()));

        // Final Cleanup
        fs::remove_file(&sst_a_path).unwrap();
        fs::remove_file(&sst_b_path).unwrap();
        fs::remove_file(&sst_c_path).unwrap();
        fs::remove_file(&final_path).unwrap();
    }

    #[test]
    fn test_compaction_with_simd() {
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
            SSTable::write(&sst1_path, data1, None, CompressionPolicy::Balanced).unwrap();

            // Compact (even single SSTable to test the path)
            Compactor::compact(&[&sst1_path], &merged_path, None, CompressionPolicy::Balanced).unwrap();

            // Verify
            let merged = SSTable::open(&merged_path, None).unwrap();
            assert_eq!(merged.get(b"ts1").unwrap(), Some(original_values));

            // Cleanup
            fs::remove_file(&sst1_path).unwrap();
            fs::remove_file(&merged_path).unwrap();
        }
    }
}
