#[cfg(feature = "default-server")]
use crate::server::test_server::run_test_server;

// External server should be already running, we just return its address
#[cfg(not(feature = "default-server"))]
use crate::tests::external_server::run_test_server;

use super::snow_sql::{SnowSqlCommand, snow_sql};
use crate::models::JsonResponse;

fn insta_replace_filiters() -> Vec<(&'static str, &'static str)> {
    vec![(
        r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
        "UUID",
    )]
}

const DEMO_USER: &str = "embucket";
const DEMO_PASSWORD: &str = "embucket";

fn query_id_from_snapshot(
    snapshot: &JsonResponse,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    if let Some(data) = &snapshot.data {
        if let Some(query_id) = &data.query_id {
            Ok(query_id.clone())
        } else {
            Err("No query ID".into())
        }
    } else {
        Err("No data".into())
    }
}

#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sqls:expr) => {
        #[tokio::test]
        async fn $name() {
            let mod_name = module_path!().split("::").last().unwrap();
            let server_addr = run_test_server(DEMO_USER, DEMO_PASSWORD).await;
            let mut prev_response: Option<JsonResponse> = None;
            for sql in $sqls {
                let mut sql = sql.to_string();

                // replace $LAST_QUERY_ID by query_id from previous response
                if sql.contains("$LAST_QUERY_ID") {
                    let resp = prev_response.expect("No previous response");
                    let last_query_id = query_id_from_snapshot(&resp).expect("Can't acquire value for $LAST_QUERY_ID");
                    sql = sql.replace("$LAST_QUERY_ID", &last_query_id);
                }

                let snapshot = snow_sql(&server_addr, DEMO_USER, DEMO_PASSWORD, SnowSqlCommand::Query(sql.to_string())).await;

                insta::with_settings!({
                    snapshot_path => format!("snapshots/{mod_name}"),
                    // for debug purposes fetch query_id of current query
                    description => format!("SQL: {}\nQuery UUID: {}", sql,
                        query_id_from_snapshot(&snapshot)
                            .map_or_else(|_| "No query ID".to_string(), |id| id)),
                    sort_maps => true,
                    filters => insta_replace_filiters(),
                    // info => &historical_codes,
                }, {
                    insta::assert_json_snapshot!(snapshot);
                });

                prev_response = Some(snapshot);
            }
        }
    };
}

mod snowflake_compatibility {
    use super::*;

    sql_test!(
        create_table_bad_syntax,
        [
            // "Snowflake:
            // 001003 (42000): UUID: SQL compilation error:
            // syntax error line 1 at position 16 unexpected '<EOF>'."
            "create table foo",
        ]
    );

    // incorrect sql_state: 42000, should be: 42502
    sql_test!(
        select_from_missing_table,
        [
            // "Snowflake:
            // 002003 (42S02): UUID: SQL compilation error:
            // Object 'FOO' does not exist or not authorized."
            "select * from foo",
        ]
    );

    sql_test!(
        show_schemas_in_missing_db,
        [
            // "Snowflake:
            // 002043 (02000): UUID: SQL compilation error:
            // Object does not exist, or operation cannot be performed."
            "show schemas in database foo",
        ]
    );

    sql_test!(
        select_1,
        [
            // "Snowflake:
            // +---+
            // | 1 |
            // |---|
            // | 1 |
            // +---+"
            "select 1",
        ]
    );

    sql_test!(
        select_1_async,
        [
            // scheduled query ID
            "select 1;>",
            // +---+
            // | 1 |
            // |---|
            // | 1 |
            // +---+"
            "!result $LAST_QUERY_ID",
        ]
    );

    sql_test!(
        cancel_query_bad_id1,
        [
            // Invalid UUID.
            "SELECT SYSTEM$CANCEL_QUERY(1);",
        ]
    );

    sql_test!(
        cancel_query_bad_id2,
        [
            // Invalid UUID.
            "SELECT SYSTEM$CANCEL_QUERY('1');",
        ]
    );

    // sql_test!(
    //     cancel_query,
    //     [
    //         // 1: scheduled query ID
    //         "select 1;>",
    //         // 2: query [UUID] terminated.
    //         "SELECT SYSTEM$CANCEL_QUERY('$LAST_QUERY_ID');",
    //     ]
    // );
}
