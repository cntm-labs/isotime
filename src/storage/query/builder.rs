use crate::storage::query::plan::QueryPlan;
use crate::storage::StorageEngine;
use std::sync::Arc;

pub struct QueryBuilder {
    #[allow(dead_code)]
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

    pub async fn execute(self) -> QueryPlan {
        self.plan
    }
}
