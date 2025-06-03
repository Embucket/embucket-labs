use crate::session::register_session_context_udfs;
use crate::table::register_udtfs;
use crate::{register_udafs, register_udfs};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
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
        .build();
    let mut ctx = SessionContext::new_with_state(state);
    register_session_context_udfs(&mut ctx).unwrap();
    register_udfs(&mut ctx).expect("Cannot register UDFs");
    register_udafs(&mut ctx).expect("Cannot register UDAFs");
    register_udtfs(&ctx);
    Arc::new(ctx)
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

                let res = ctx.sql($query).await.unwrap().collect().await;
                let mut settings = insta::Settings::new();
                settings.set_description(stringify!($query));
                settings.set_omit_expression(true);
                settings.set_prepend_module_to_snapshot(false);
                settings.set_snapshot_path(concat!("snapshots", "/") $(.to_owned() + $user_snapshot_path)?);

                let setup: Vec<&str> = vec![$($($setup_queries),*)?];
                if !setup.is_empty() {
                    settings.set_info(&format!("Setup queries: {}", setup.join("; ")));
                }
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
