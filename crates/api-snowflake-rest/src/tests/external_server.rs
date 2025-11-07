use std::net::SocketAddr;

type AppCfg = (); // define stub, as AppCfg not linked with core-executor
type UtilsConfig = (); // define stub, as UtilsConfig not linked with core-executor
const SERVER_ADDRESS: &str = "127.0.0.1:3000";

// It is expected that embucket service is already running
pub fn run_test_rest_api_server(_: Option<(AppCfg, UtilsConfig)>) -> SocketAddr {
    SERVER_ADDRESS
        .parse::<SocketAddr>()
        .expect("Failed to parse server address")
}

pub fn server_default_cfg(_data_format: &str) -> Option<(AppCfg, UtilsConfig)> {
    // should use defaults, when using external server as we doesn't link with core-executor
    None
}
