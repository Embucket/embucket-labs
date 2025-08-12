use snafu::ResultExt;

use super::e2e_common::{Error, TestToxiProxySnafu};

const TOXIPROXY_ENDPOINT: &str = "http://localhost:8474/proxies";

pub async fn create_toxiproxy(payload: &str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(reqwest::Method::POST, TOXIPROXY_ENDPOINT.to_string())
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()
        .await
        .context(TestToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(TestToxiProxySnafu)
    }
}

pub async fn delete_toxiproxy(proxy_name: &str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(
            reqwest::Method::DELETE,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}"),
        )
        .send()
        .await
        .context(TestToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(TestToxiProxySnafu)
    }
}

pub async fn create_toxic_conn_limit(
    proxy_name: &str,
    bytes_count: usize,
) -> Result<reqwest::Response, Error> {
    // use upstream as downstream limit doesn't work properly with minio
    // probably as of retries object store is doing
    let payload = format!(
        r#"{{
        "name": "close_connection_on_limit",
        "type": "limit_data",
        "stream": "upstream",
        "attributes": {{
            "bytes": {bytes_count}
        }}
    }}"#
    );
    let client = reqwest::Client::new();
    let res = client
        .request(
            reqwest::Method::POST,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}/toxics"),
        )
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
        .context(TestToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(TestToxiProxySnafu)
    }
}

pub async fn delete_toxic_conn_limit(proxy_name: &str) -> Result<reqwest::Response, Error> {
    let client = reqwest::Client::new();
    let res = client
        .request(
            reqwest::Method::DELETE,
            format!("{TOXIPROXY_ENDPOINT}/{proxy_name}/toxics/close_connection_on_limit"),
        )
        .send()
        .await
        .context(TestToxiProxySnafu)?;
    if res.status().is_success() {
        Ok(res)
    } else {
        res.error_for_status().context(TestToxiProxySnafu)
    }
}
