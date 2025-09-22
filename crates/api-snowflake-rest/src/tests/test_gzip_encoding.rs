#[cfg(test)]
#[cfg(not(feature = "external-server"))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
mod tests {
    use crate::models::{
        ClientEnvironment, JsonResponse, LoginRequestBody, LoginRequestData, LoginResponse,
        QueryRequestBody,
    };
    use crate::server::test_server::run_test_rest_api_server;
    use axum::body::Bytes;
    use axum::http;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use reqwest::Method;
    use reqwest::header::AUTHORIZATION;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::io::Write;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_login() {
        let addr = run_test_rest_api_server().await;
        let client = reqwest::Client::new();
        let login_url = format!("http://{addr}/session/v1/login-request");
        let query_url = format!("http://{addr}/queries/v1/query-request");

        let query_request = QueryRequestBody {
            sql_text: "SELECT 1;".to_string(),
            async_exec: false,
        };

        let query_compressed_bytes = make_bytes_body(&query_request);

        assert!(
            !query_compressed_bytes.is_empty(),
            "Compressed data should not be empty"
        );

        //Check before login without an auth header
        let res = client
            .request(
                Method::POST,
                format!("{query_url}?requestId={}", Uuid::new_v4()),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::UNAUTHORIZED, res.status());

        let login_request = LoginRequestBody {
            data: LoginRequestData {
                client_app_id: String::new(),
                client_app_version: String::new(),
                svn_revision: None,
                account_name: String::new(),
                login_name: "embucket".to_string(),
                client_environment: ClientEnvironment {
                    application: String::new(),
                    os: String::new(),
                    os_version: String::new(),
                    python_version: String::new(),
                    python_runtime: String::new(),
                    python_compiler: String::new(),
                    ocsp_mode: String::new(),
                    tracing: 0,
                    login_timeout: None,
                    network_timeout: None,
                    socket_timeout: None,
                },
                password: "embucket".to_string(),
                session_parameters: HashMap::default(),
            },
        };

        let login_compressed_bytes = make_bytes_body(&login_request);

        assert!(
            !login_compressed_bytes.is_empty(),
            "Compressed data should not be empty"
        );

        //Login
        let res = client
            .request(
                Method::POST,
                format!(
                    "{login_url}?request_id=123&databaseName=embucket&schemaName=public&warehouse=embucket"
                ),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(login_compressed_bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::OK, res.status());
        let login_response: LoginResponse = res.json().await.unwrap();
        assert!(login_response.data.is_some());
        assert!(login_response.success);
        assert!(login_response.message.is_some());

        //Check after login without an auth header
        let res = client
            .request(
                Method::POST,
                format!("{query_url}?requestId={}", Uuid::new_v4()),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::UNAUTHORIZED, res.status());

        //Check after login with an auth header
        let res = client
            .request(
                Method::POST,
                format!("{query_url}?requestId={}", Uuid::new_v4()),
            )
            .header(
                AUTHORIZATION,
                format!("Snowflake Token=\"{}\"", login_response.data.unwrap().token),
            )
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(query_compressed_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::OK, res.status());
        let query_response: JsonResponse = res.json().await.unwrap();
        assert!(query_response.data.is_some());
        assert!(query_response.success);
        assert!(query_response.message.is_some());
        assert!(query_response.code.is_none()); // no code set on success
    }
    fn make_bytes_body<T: ?Sized + Serialize>(request: &T) -> Bytes {
        let json = serde_json::to_string(request).expect("Failed to serialize JSON");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(json.as_bytes())
            .expect("Failed to write to encoder");
        let compressed_data = encoder.finish().expect("Failed to finish compression");

        Bytes::from(compressed_data)
    }
}
