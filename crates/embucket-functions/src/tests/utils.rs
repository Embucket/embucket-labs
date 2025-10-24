use crate::expr_planner::CustomExprPlanner;
use crate::session::register_session_context_udfs;
use crate::session_params::SessionParams;
use crate::table::register_udtfs;
use crate::{register_udafs, register_udfs};
use core_history::{HistoryStore, MockHistoryStore, QueryRecord, QueryRecordId};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use snafu::Location;
use std::sync::Arc;

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub fn create_session() -> Arc<SessionContext> {
    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_create_default_catalog_and_schema(true)
                .set_bool(
                    "datafusion.execution.skip_physical_aggregate_schema_check",
                    true,
                ),
        )
        .with_default_features()
        .with_expr_planners(vec![Arc::new(CustomExprPlanner)])
        .build();
    let mut ctx = SessionContext::new_with_state(state);
    let session_params = Arc::new(SessionParams::default());
    register_session_context_udfs(&mut ctx, &session_params).unwrap();
    register_udfs(&mut ctx, &session_params).expect("Cannot register UDFs");
    register_udafs(&mut ctx).expect("Cannot register UDAFs");
    register_udtfs(&ctx, history_store_mock());
    Arc::new(ctx)
}
pub fn history_store_mock() -> Arc<dyn HistoryStore> {
    let mut mock = MockHistoryStore::new();
    mock.expect_get_queries().returning(|_| {
        let mut records = Vec::new();
        for i in 0..4 {
            let mut q = QueryRecord::new("query", None);
            q.id = i.into();
            records.push(q);
        }
        Ok(records)
    });
    mock.expect_get_query().returning(|id| {
        let mut record = QueryRecord::new("query", None);
        let buf = r#"
        {
            "columns": [{"name":"a","type":"text"},{"name":"b","type":"text"},{"name":"c","type":"text"}],
            "rows": [[1,"2",true],[2.0,"4",false]],
            "data_format": "arrow",
            "schema": "{\"fields\":[{\"name\":\"a\",\"data_type\":\"Float64\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"b\",\"data_type\":\"Utf8\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"c\",\"data_type\":\"Boolean\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}}],\"metadata\":{}}"
        }"#;
        record.result = Some(buf.to_string());
        if id == QueryRecordId(500) {
            return Err(core_history::Error::ExecutionResult {
                message: "Query not found".to_string(),
                location: Location::default(),
            });
        }
        if id == QueryRecordId(100) {
            record.error = Some("query error".to_string());
        }
        Ok(record)
    });
    let history_store: Arc<dyn HistoryStore> = Arc::new(mock);
    history_store
}

#[macro_export]
macro_rules! test_query {
    (
        $test_fn_name:ident,
        $query:expr
        $(, setup_queries =[$($setup_queries:expr),* $(,)?])?
        $(, snapshot_path = $user_snapshot_path:expr)?
    ) => {
        paste::paste! {
            #[tokio::test]
            async fn [< query_ $test_fn_name >]() {
                let ctx = $crate::tests::utils::create_session();

                // Execute all setup queries (if provided) to set up the session context
                $(
                    $(
                        {
                            ctx.sql($setup_queries).await.unwrap().collect().await.unwrap();
                        }
                    )*
                )?
                let mut settings = insta::Settings::new();
                settings.set_description(stringify!($query));
                settings.set_omit_expression(true);
                settings.set_prepend_module_to_snapshot(false);
                settings.set_snapshot_path(concat!("snapshots", "/") $(.to_owned() + $user_snapshot_path)?);

                let setup: Vec<&str> = vec![$($($setup_queries),*)?];
                if !setup.is_empty() {
                    settings.set_info(&format!("Setup queries: {}", setup.join("; ")));
                }

                // Some queries may fail during Dataframe preparing, so we need to check for errors
                let res = ctx.sql($query).await;
                if let Err(ref e) = res {
                    let err = format!("Error: {}", e);
                    settings.bind(|| {
                        insta::assert_debug_snapshot!(err);
                    });
                    return
                }
                let res = res.unwrap().collect().await;

                settings.bind(|| {
                    let df = match res {
                        Ok(record_batches) => {
                            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string())
                        },
                        Err(e) => Err(format!("Error: {e}"))
                    };

                    let df = df.map(|df| df.split('\n').map(|s| s.to_string()).collect::<Vec<String>>());
                    insta::assert_debug_snapshot!((df));
                });
            }
        }
    };
}
