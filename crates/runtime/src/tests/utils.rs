use std::sync::Arc;

use crate::execution::{query::QueryContext, session::UserSession};
use embucket_metastore::{
    Database as MetastoreDatabase, Metastore, Schema as MetastoreSchema,
    SchemaIdent as MetastoreSchemaIdent, SlateDBMetastore, Volume as MetastoreVolume,
};

static TABLE_SETUP: &str = include_str!(r"./queries/table_setup.sql");

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn create_df_session() -> Arc<UserSession> {
    let metastore = SlateDBMetastore::new_in_memory().await;
    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                embucket_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"embucket".to_string(),
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let user_session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );

    for query in TABLE_SETUP.split(';') {
        if !query.is_empty() {
            let mut query = user_session.query(query, QueryContext::default());
            query.execute().await.unwrap();
            //ctx.sql(query).await.unwrap().collect().await.unwrap();
        }
    }
    user_session
}

pub mod macros {
    macro_rules! test_query {
        ($test_fn_name:ident, $query:expr) => {
            paste::paste! {
                #[tokio::test]
                async fn [< query_ $test_fn_name >]() {
                    let ctx = crate::tests::utils::create_df_session().await;

                    let mut query = ctx.query($query, crate::execution::query::QueryContext::default());
                    let res = query.execute().await;
                    insta::with_settings!({
                        description => stringify!($query),
                        omit_expression => true,
                        prepend_module_to_snapshot => false
                    }, {
                        let df =  match res {
                            Ok(record_batches) => {
                                Ok(datafusion::arrow::util::pretty::pretty_format_batches(&record_batches).unwrap().to_string())
                            },
                            Err(e) => Err(format!("Error: {e}"))
                        };
                        let df = df.map(|df| df.split("\n").map(|s| s.to_string()).collect::<Vec<String>>());
                        insta::assert_debug_snapshot!((df));
                    })
                }
            }
        }
    }

    pub(crate) use test_query;
}
