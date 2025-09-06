use std::net::SocketAddr;

const SERVER_ADDRESS: &str = "127.0.0.1:3000";

pub async fn run_test_server() -> SocketAddr {
    SERVER_ADDRESS
        .parse::<SocketAddr>()
        .expect("Failed to parse server address")
}
