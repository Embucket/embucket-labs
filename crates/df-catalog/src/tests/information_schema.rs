use crate::catalog_list::{DEFAULT_CATALOG, EmbucketCatalogList};
use crate::information_schema::information_schema::{
    INFORMATION_SCHEMA, InformationSchemaProvider,
};
use core_metastore::SlateDBMetastore;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

#[allow(clippy::unwrap_used)]
async fn create_session_context() -> Arc<SessionContext> {
    let metastore = SlateDBMetastore::new_in_memory().await;
    let catalog_list_impl = Arc::new(EmbucketCatalogList::new(metastore.clone()));

    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_create_default_catalog_and_schema(true)
                .set_str("datafusion.catalog.default_catalog", DEFAULT_CATALOG),
        )
        .with_catalog_list(catalog_list_impl.clone())
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.catalog(DEFAULT_CATALOG)
        .unwrap()
        .register_schema(
            INFORMATION_SCHEMA,
            Arc::new(InformationSchemaProvider::new(
                catalog_list_impl.clone(),
                Arc::from(DEFAULT_CATALOG),
            )),
        )
        .unwrap();
    ctx.sql("CREATE TABLE test (id INT)").await.unwrap();
    Arc::new(ctx)
}

#[macro_export]
macro_rules! test_query {
    ($test_name:ident, $query:expr) => {
        paste::paste! {
            #[tokio::test]
            async fn [< test_ $test_name >]() {
                let ctx = create_session_context().await;
                let record_batches = ctx
                    .sql($query)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                insta::with_settings!({
                    description => $query,
                    omit_expression => true,
                    prepend_module_to_snapshot => false
                }, {
                    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string();
                    insta::assert_snapshot!(stringify!($test_name), formatted);
                })
            }
        }
    };
}

test_query!(
    information_schema_tables,
    "SELECT * FROM embucket.information_schema.tables "
);
test_query!(
    information_schema_databases,
    "SELECT * FROM embucket.information_schema.databases"
);
test_query!(
    information_schema_columns,
    "SELECT * FROM embucket.information_schema.columns"
);
test_query!(
    information_schema_views,
    "SELECT * FROM embucket.information_schema.views"
);
test_query!(
    information_schema_schemata,
    "SELECT * FROM embucket.information_schema.schemata"
);
test_query!(
    information_schema_df_settings,
    "SELECT * FROM embucket.information_schema.df_settings"
);

test_query!(
    information_schema_navigation_tree,
    "SELECT * FROM embucket.information_schema.navigation_tree ORDER BY database, schema, table"
);

// These information_schema tables (routines, parameters) can return rows in a non-deterministic order,
// causing snapshot tests to randomly fail.
// To avoid flakiness, we group results by primary identifying columns and count the rows per group.
// This makes the output deterministic and ensures stable test snapshots.
test_query!(
    information_schema_routines,
    "SELECT routine_name FROM embucket.information_schema.routines \
     GROUP BY routine_name \
     ORDER BY routine_name"
);

test_query!(
    information_schema_parameters,
    "SELECT specific_name FROM embucket.information_schema.parameters \
     GROUP BY specific_name \
     ORDER BY specific_name"
);
