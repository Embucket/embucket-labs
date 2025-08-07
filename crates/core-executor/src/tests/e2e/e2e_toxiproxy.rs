use snafu::ResultExt;

use super::e2e_common::{Error, ToxiProxySnafu};

const TOXIPROXY_ENDPOINT:&str = "http://localhost:8474/proxies";

#[must_use]
pub async fn create_toxiproxy(payload:&str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(reqwest::Method::POST,
            format!("{TOXIPROXY_ENDPOINT}"))
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()
        .await
        .context(ToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(ToxiProxySnafu)
    }
}

#[must_use]
pub async fn delete_toxiproxy(proxy_name:&str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(reqwest::Method::DELETE,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}"))
        .send()
        .await
        .context(ToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(ToxiProxySnafu)
    }
}

pub async fn create_toxic_conn_limit(proxy_name:&str, bytes_count: usize) -> Result<reqwest::Response, Error> {
    let payload = format!(r#"{{
        "name": "close_connection_on_limit",
        "type": "limit_data",
        "stream": "downstream",
        "attributes": {{
            "bytes": {bytes_count}
        }}
    }}"#);
    let client = reqwest::Client::new();
    let res = client
        .request(reqwest::Method::POST,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}/toxics"))
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
        .context(ToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(ToxiProxySnafu)
    }
}

pub async fn delete_toxic_conn_limit(proxy_name:&str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(reqwest::Method::DELETE,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}/toxics/close_connection_on_limit"))
        .send()
        .await
        .context(ToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(ToxiProxySnafu)
    }
}
