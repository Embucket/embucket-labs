use std::net::SocketAddr;

const SERVER_ADDRESS: &str = "127.0.0.1:3000";

// It is expected that embucket service is already running
pub async fn run_test_server(_demo_user: &str, _demo_password: &str) -> SocketAddr {
    SERVER_ADDRESS
        .parse::<SocketAddr>()
        .expect("Failed to parse server address")
}
