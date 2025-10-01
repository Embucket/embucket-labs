use crate::server::server_models::Config;
use crate::server::test_server::run_test_rest_api_server_with_config;
use crate::sql_test;
use core_executor::utils::Config as UtilsConfig;
use std::net::SocketAddr;

// These tests will be compiled / executed us usually. They spawn own server on every test.
// In case you need faster development cycle - go to test_rest_sqls.rs

pub async fn run_test_rest_api_server(data_format: &str) -> SocketAddr {
    let app_cfg = Config::new(data_format)
        .expect("Failed to create config")
        .with_demo_credentials("embucket".to_string(), "embucket".to_string());
    let execution_cfg = UtilsConfig::default()
        .with_max_concurrency_level(2)
        .with_query_timeout(1)
        .with_query_history_rows_limit(5);

    run_test_rest_api_server_with_config(app_cfg, execution_cfg).await
}

mod snowflake_generic {
    use super::*;
    use crate::tests::sql_macro::{ARROW, JSON};

    sql_test!(
        JSON,
        submit_ok_query_with_concurrent_limit,
        [
            // 1: scheduled query ID
            "SELECT sleep(1);>",
            // 2: scheduled query ID
            "SELECT sleep(1);>",
            // 3: concurrent limit exceeded
            "SELECT sleep(1);>",
        ]
    );

    // first test of arrow server
    sql_test!(
        ARROW,
        select_date_timestamp_in_arrow_format,
        ["SELECT TO_DATE('2022-08-19', 'YYYY-MM-DD'), CAST('2022-08-19-00:00' AS TIMESTAMP)"]
    );

    sql_test!(
        JSON,
        set_variable_query_history_rows_limit,
        [
            "select * from values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
            // should be just 5 rows in history
            "!result $LAST_QUERY_ID",
        ]
    );
}
