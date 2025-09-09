use datafusion::optimizer::{Analyzer, AnalyzerRule};
use embucket_functions::session_params::SessionParams;
use std::sync::Arc;

pub mod cast_analyzer;
pub mod iceberg_types_analyzer;
pub mod like_ilike_type_analyzer;
pub mod union_schema_analyzer;

#[must_use]
pub fn analyzer_rules(
    session_params: Arc<SessionParams>,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    //Ordering matters a lot, including `.extend(...)`
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(like_ilike_type_analyzer::LikeILikeTypeAnalyzer {}),
        Arc::new(iceberg_types_analyzer::IcebergTypesAnalyzer {}),
        Arc::new(cast_analyzer::CastAnalyzer::new(session_params)),
    ];

    let after_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        // Must be registered after CastAnalyzer because it introduces function calls
        // that can change the schema
        Arc::new(union_schema_analyzer::UnionSchemaAnalyzer::new()),
    ];

    rules.extend(Analyzer::new().rules);

    rules.extend(after_rules);

    rules
}
