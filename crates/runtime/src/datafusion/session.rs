use crate::datafusion::type_planner::CustomTypePlanner;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_iceberg::planner::IcebergQueryPlanner;
use std::env;
use std::sync::Arc;

pub struct Session {
    pub ctx: SessionContext,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    #[must_use]
    pub fn new() -> Self {
        let sql_parser_dialect =
            env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_information_schema(true)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect),
            )
            .with_default_features()
            .with_query_planner(Arc::new(IcebergQueryPlanner {}))
            .with_type_planner(Arc::new(CustomTypePlanner {}))
            .build();
        let ctx = SessionContext::new_with_state(state);
        Self { ctx }
    }
}
