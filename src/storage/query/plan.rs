#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    pub tag: Option<String>,
    pub value_range: Option<(f64, f64)>,
    pub causal_after: Option<Vec<(u32, u64)>>,
    pub limit: usize,
}
