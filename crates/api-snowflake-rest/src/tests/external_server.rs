use std::net::SocketAddr;

const SERVER_ADDRESS: &str = "127.0.0.1:3000";

// It is expected that embucket service is already running
pub async fn run_test_rest_api_server(data_format: &str) -> SocketAddr {
    // for external test server JSON data format is expected by default
    assert_eq!(data_format.to_ascii_lowercase(), "json");
    SERVER_ADDRESS
        .parse::<SocketAddr>()
        .expect("Failed to parse server address")
}
