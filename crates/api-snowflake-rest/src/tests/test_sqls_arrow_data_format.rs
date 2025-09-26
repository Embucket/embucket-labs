use crate::server::server_models::Config;
use crate::server::test_server::run_test_rest_api_server_with_config;
use crate::sql_test;
use core_executor::utils::Config as UtilsConfig;
use std::net::SocketAddr;

// These tests will be compiled / executed us usually. They spawn own server on every test.
// In case you need faster development cycle - go to test_rest_sqls.rs

pub async fn run_test_rest_api_server(data_format: &str) -> SocketAddr {
    let app_cfg =
        Config::new(data_format)
            .expect("Failed to create config")
            .with_demo_credentials("embucket".to_string(), "embucket".to_string());
    let execution_cfg = UtilsConfig::default()
        .with_max_concurrency_level(2)
        .with_query_timeout(1);

    run_test_rest_api_server_with_config(app_cfg, execution_cfg).await
}

mod arrow_data_format_tests {
    use super::*;
    use crate::tests::sql_macro::ARROW;

    sql_test!(
        ARROW,
        empty,
        [
            "SELECT CURRENT_DATE(), CAST('2022-08-19-00:00' AS TIMESTAMP)"
        ]
    );

}
