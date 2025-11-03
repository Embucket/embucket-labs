use crate::server::test_server::run_test_rest_api_server;
use crate::server::server_models::Config as AppCfg;
use core_executor::utils::Config as UtilsConfig;
use crate::sql_test;

// These tests will be compiled / executed us usually. They spawn own server on every test.
// In case you need faster development cycle - go to test_rest_sqls.rs

// Below configs will be used by tests defined in this file only.

fn server_custom_cfg(data_format: &str) -> Option<(AppCfg, UtilsConfig)> {
    Some((
        AppCfg::new(data_format)
            .expect("Failed to create server config")
            .with_demo_credentials("embucket".to_string(), "embucket".to_string()),
        UtilsConfig::default()
            .with_max_concurrency_level(2)
            .with_query_timeout(2)
            .with_query_history_rows_limit(5),
    ))
}

mod snowflake_generic {
    use super::*;
    use crate::tests::sql_macro::{ARROW, JSON};

    sql_test!(
        server_custom_cfg(JSON),
        submit_ok_query_with_concurrent_limit,
        [
            // 1: scheduled query ID
            "SELECT sleep(2);>",
            // 2: scheduled query ID
            "SELECT sleep(2);>",
            // 3: concurrent limit exceeded
            "SELECT 1;>",
        ]
    );

    // first test of arrow server
    sql_test!(
        server_custom_cfg(ARROW),
        select_date_timestamp_in_arrow_format,
        ["SELECT TO_DATE('2022-08-19', 'YYYY-MM-DD'), CAST('2022-08-19-00:00' AS TIMESTAMP)"]
    );

    sql_test!(
        server_custom_cfg(JSON),
        set_variable_query_history_rows_limit,
        [
            "select * from values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
            // should be just 5 rows in history
            "!result $LAST_QUERY_ID",
        ]
    );
}
