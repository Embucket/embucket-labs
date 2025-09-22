use crate::server::server_models::Config;
use crate::server::test_server::run_test_rest_api_server_with_config;
use crate::sql_test;
use core_executor::utils::Config as UtilsConfig;
use std::net::SocketAddr;

// These tests will be compiled / executed us usually. They spawn own server on every test.
// In case you need faster development cycle - go to test_rest_sqls.rs

pub async fn run_test_rest_api_server() -> SocketAddr {
    let app_cfg =
        Config::default().with_demo_credentials("embucket".to_string(), "embucket".to_string());
    let execution_cfg = UtilsConfig::default()
        .with_max_concurrency_level(2)
        .with_query_timeout(1);

    run_test_rest_api_server_with_config(app_cfg, execution_cfg).await
}

mod snowflake_generic {
    use super::*;

    sql_test!(
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
}
