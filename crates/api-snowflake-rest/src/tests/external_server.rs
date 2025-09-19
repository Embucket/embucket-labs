use std::net::SocketAddr;

const SERVER_ADDRESS: &str = "127.0.0.1:3000";

// It is expected that embucket service is already running
pub async fn run_test_rest_api_server() -> SocketAddr {
    SERVER_ADDRESS
        .parse::<SocketAddr>()
        .expect("Failed to parse server address")
}
