use std::net::{SocketAddr, ToSocketAddrs};

/// Resolves the given addr to an IPv4 `SocketAddr`.
pub fn resolve_ipv4<A: ToSocketAddrs>(addr: A) -> std::io::Result<SocketAddr> {
    let addrs = addr
        .to_socket_addrs()?
        .filter(SocketAddr::is_ipv4)
        .collect::<Vec<_>>();

    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        ));
    }

    Ok(addrs[0])
}
