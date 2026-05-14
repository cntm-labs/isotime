use crate::storage::query::plan::QueryPlan;
use crate::storage::sstable::SSTable;
use crate::storage::StorageEngine;
use futures_util::future::join_all;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct QueryBuilder {
    engine: Arc<StorageEngine>,
    plan: QueryPlan,
}

impl QueryBuilder {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            plan: QueryPlan::default(),
        }
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.plan.tag = Some(tag.into());
        self
    }

    pub fn range(mut self, min: f64, max: f64) -> Self {
        self.plan.value_range = Some((min, max));
        self
    }

    pub fn after(mut self, causal_after: Vec<(u32, u64)>) -> Self {
        self.plan.causal_after = Some(causal_after);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.plan.limit = limit;
        self
    }

    pub async fn execute(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut merged = BTreeMap::new();

        // 1. Scan MemTable (Newest data)
        let (mem_snapshot, mem_tags) = self.engine.memtable.snapshot();
        let target_keys = if let Some(ref tag) = self.plan.tag {
            mem_tags.get(tag).cloned().unwrap_or_default()
        } else {
            mem_snapshot.keys().cloned().collect()
        };

        for key in target_keys {
            if let Some((val, clock)) = mem_snapshot.get(&key) {
                if self.matches_plan(val, clock) {
                    merged.insert(key, val.clone());
                }
            }
        }

        // 2. Scan SSTables in parallel
        let metas = {
            let guard = self.engine.metadatas.lock().await;
            guard.clone()
        };

        let mut tasks = Vec::new();
        for meta in metas {
            // Prune by value range if metadata is available
            if let Some((q_min, q_max)) = self.plan.value_range {
                if let (Some(f_min), Some(f_max)) = (meta.min_val, meta.max_val) {
                    if f_max < q_min || f_min > q_max {
                        continue; // No overlap
                    }
                }
            }

            let engine = Arc::clone(&self.engine);
            let plan = self.plan.clone();
            tasks.push(tokio::spawn(async move {
                let sstable = SSTable::open(&meta.path, engine.encryption.as_deref(), &engine.io_pool)
                    .await?;
                
                let mut local_results = Vec::new();
                let keys = if let Some(ref tag) = plan.tag {
                    sstable.get_by_tag(tag).await?
                } else {
                    // If no tag, we have to scan all entries for this SSTable 
                    // (In a real system we might use index for other filters)
                    sstable.all_entries(Some(&engine.cas)).await?.into_iter().map(|(k, _)| k).collect()
                };

                for key in keys {
                    if let Some((val, clock)) = sstable.get_with_clock(&key, Some(&engine.cas)).await? {
                        if Self::matches_static(&plan, &val, &clock) {
                            local_results.push((key, val));
                        }
                    }
                }
                Ok::<Vec<(Vec<u8>, Vec<u8>)>, std::io::Error>(local_results)
            }));
        }

        let task_results = join_all(tasks).await;
        for tr in task_results {
            if let Ok(Ok(results)) = tr {
                for (k, v) in results {
                    // Newest (MemTable) already in 'merged'. Since SSTables are typically older,
                    // we only insert if not present OR if we were doing a more complex merge.
                    // For this causal engine, newer SSTables (L0) should overwrite older ones (L3).
                    // But MemTable is newest.
                    merged.entry(k).or_insert(v);
                }
            }
        }

        // 3. Final filtering: Remove tombstones and apply limit
        let mut final_results: Vec<_> = merged
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        if self.plan.limit > 0 && final_results.len() > self.plan.limit {
            final_results.truncate(self.plan.limit);
        }

        final_results
    }

    fn matches_plan(&self, value: &[u8], clock: &[(u32, u64)]) -> bool {
        Self::matches_static(&self.plan, value, clock)
    }

    fn matches_static(plan: &QueryPlan, value: &[u8], clock: &[(u32, u64)]) -> bool {
        // Range check
        if let Some((min, max)) = plan.value_range {
            if value.len() == 8 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(value);
                let v = f64::from_le_bytes(bytes);
                if v < min || v > max {
                    return false;
                }
            } else {
                return false; // Not a numeric value
            }
        }

        // Causal check
        if let Some(ref after) = plan.causal_after {
            if !vector_clock_gt(clock, after) {
                return false;
            }
        }

        true
    }
}

/// Returns true if A > B in Vector Clock terms.
/// A > B iff for all i, A[i] >= B[i] AND there exists j such that A[j] > B[j].
fn vector_clock_gt(a: &[(u32, u64)], b: &[(u32, u64)]) -> bool {
    let mut a_map = BTreeMap::new();
    for &(node, count) in a {
        a_map.insert(node, count);
    }

    let mut b_map = BTreeMap::new();
    for &(node, count) in b {
        b_map.insert(node, count);
    }

    let mut strictly_greater = false;

    // Check all nodes in B are covered by A with >= counters
    for (node, b_count) in &b_map {
        let a_count = a_map.get(node).cloned().unwrap_or(0);
        if a_count < *b_count {
            return false;
        }
        if a_count > *b_count {
            strictly_greater = true;
        }
    }

    // Also check if A has any nodes not in B with counters > 0
    if !strictly_greater {
        for (node, a_count) in &a_map {
            if *a_count > 0 && !b_map.contains_key(node) {
                strictly_greater = true;
                break;
            }
        }
    }

    strictly_greater
}
