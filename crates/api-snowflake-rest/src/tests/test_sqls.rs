#[cfg(not(feature = "external-server"))]
use crate::server::test_server::run_test_server;

// External server should be already running, we just return its address
#[cfg(feature = "external-server")]
use crate::tests::external_server::run_test_server;

use super::client::{get_query_result, login, query};
use crate::models::{JsonResponse, LoginResponse};
use http::header;
use std::net::SocketAddr;

fn insta_replace_filiters() -> Vec<(&'static str, &'static str)> {
    vec![(
        r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
        "UUID",
    )]
}

const DEMO_USER: &str = "embucket";
const DEMO_PASSWORD: &str = "embucket";

#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sql:expr, $meta:expr) => {
        #[tokio::test]
        async fn $name() {
            let meta = $meta.lines()
                    .map(|line| line.trim_start())
                    .collect::<Vec<_>>()
                    .join("\n");
            let server_addr = run_test_server(DEMO_USER, DEMO_PASSWORD).await;
            let (snapshot, query_record) = snow_sql(&server_addr, $sql).await;

            let query_id = &snapshot.data.as_ref().map_or_else(
                || { format!("No data") },
                |data| { format!(
                    "Query UUID: {}",
                    data.query_id.clone().unwrap_or("No query ID".to_string())
                )},
            );

            let historical_codes = if let Some(data) = &query_record.data {
                format!("historical codes: sqlState: {}, errorCode: {}",
                    data.sql_state.as_ref().unwrap_or(&"".to_string()),
                    data.error_code.as_ref().unwrap_or(&"".to_string()))
            } else {
                "No data".to_string()
            };

            insta::with_settings!({
                snapshot_path => "snapshots",
                description => format!("{}\n{}\n{}", $sql, query_id, meta),
                sort_maps => true,
                filters => insta_replace_filiters(),
                info => &historical_codes,
            }, {
                insta::assert_json_snapshot!(snapshot);
            });
        }
    };
}

// Objectives: test error_code, sql_status, error message, result data.
// with insta snapshots
pub async fn snow_sql(server_addr: &SocketAddr, sql: &str) -> (JsonResponse, JsonResponse) {
    let client = reqwest::Client::new();
    let (headers, login_res) =
        login::<LoginResponse>(&client, server_addr, DEMO_USER, DEMO_PASSWORD)
            .await
            .expect("Failed to login");
    assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

    let access_token = login_res
        .data
        .clone()
        .map_or_else(String::new, |data| data.token);

    let mut async_exec = false;
    let mut sql = sql;
    // if sql ends with ;> it is async query
    if sql.ends_with(";>") {
        async_exec = true;
        sql = sql.trim_end_matches(";>");
    }
    let (_headers, res) =
        query::<JsonResponse>(&client, server_addr, &access_token, sql, async_exec)
            .await
            .expect("Failed to run query");

    let query_id = if let Some(data) = &res.data {
        data.query_id.clone().unwrap_or_default()
    } else {
        "No query ID".to_string()
    };
    let (_headers, history) =
        get_query_result::<JsonResponse>(&client, server_addr, &access_token, &query_id)
            .await
            .expect("Failed to get query result");

    (res, history)
}

mod snowflake_compatibility {
    use super::*;

    sql_test!(
        create_table_bad_syntax,
        "create table foo",
        "Snowflake:
        001003 (42000): UUID: SQL compilation error: 
        syntax error line 1 at position 16 unexpected '<EOF>'."
    );

    // incorrect sql_state: 42000, should be: 42502
    sql_test!(
        select_from_missing_table,
        "select * from foo",
        "Snowflake:
        002003 (42S02): UUID: SQL compilation error:
        Object 'FOO' does not exist or not authorized."
    );

    sql_test!(
        show_schemas_in_missing_db,
        "show schemas in database foo",
        "Snowflake:
        002043 (02000): UUID: SQL compilation error:
        Object does not exist, or operation cannot be performed."
    );

    sql_test!(
        select_1,
        "select 1",
        "Snowflake: 
        +---+
        | 1 |
        |---|
        | 1 |
        +---+"
    );

    sql_test!(
        select_1_async,
        "select 1;>",
        "Snowflake: scheduled query ID"
    );
}
