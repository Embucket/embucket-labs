#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
mod tests {
    use crate::models::{JsonResponse, LoginResponse};
    use crate::server::test_server::run_test_rest_api_server;
    use crate::tests::client::{abort, get_query_result, login, query};
    use crate::tests::sql_macro::{JSON, query_id_from_snapshot};
    use crate::server::test_server::server_default_cfg;
    use axum::http;
    use http::header;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_by_request_id() {
        let addr = run_test_rest_api_server(server_default_cfg(JSON));

        let client = reqwest::Client::new();

        let (headers, login_res) = login::<LoginResponse>(&client, &addr, "embucket", "embucket")
            .await
            .expect("Failed to login");
        assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

        let access_token = login_res
            .data
            .clone()
            .map_or_else(String::new, |data| data.token);

        let request_id = Uuid::new_v4();
        let sql = "SELECT SLEEP(1)";
        let (_headers, res) =
            query::<JsonResponse>(&client, &addr, &access_token, request_id, 0, sql, true)
                .await
                .expect("Failed to run query");
        let query_id = query_id_from_snapshot(&res).expect("Failed to get query ID");

        let (_headers, _res) = abort::<()>(&client, &addr, &access_token, request_id, sql)
            .await
            .expect("Failed to abort query");

        let (_headers, res) =
            get_query_result::<JsonResponse>(&client, &addr, &access_token, &query_id)
                .await
                .expect("Failed to get query result");
        assert_eq!(res.message, Some(format!("Query {query_id} cancelled")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_using_wrong_request_id() {
        let addr = run_test_rest_api_server(server_default_cfg(JSON));
        let client = reqwest::Client::new();

        let (headers, login_res) = login::<LoginResponse>(&client, &addr, "embucket", "embucket")
            .await
            .expect("Failed to login");
        assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

        let access_token = login_res
            .data
            .clone()
            .map_or_else(String::new, |data| data.token);

        let request_id = Uuid::new_v4();
        let sql = "SELECT SLEEP(1)";
        let (_headers, _res) =
            query::<JsonResponse>(&client, &addr, &access_token, request_id, 0, sql, true)
                .await
                .expect("Failed to run query");

        let _err = abort::<()>(&client, &addr, &access_token, Uuid::new_v4(), sql)
            .await
            .expect_err("abort query should fail");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_abort_and_retry() {
        let addr = run_test_rest_api_server(server_default_cfg(JSON));
        // let addr = "127.0.0.1:3000".parse::<std::net::SocketAddr>()
        //    .expect("Failed to parse server address");

        // it has no timeout
        let login_client = reqwest::Client::new();

        let (headers, login_res) =
            login::<LoginResponse>(&login_client, &addr, "embucket", "embucket")
                .await
                .expect("Failed to login");
        assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

        let access_token = login_res
            .data
            .clone()
            .map_or_else(String::new, |data| data.token);

        let query_client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .expect("Failed to create reqwest client, with enabled timeout");

        let request_id = Uuid::new_v4();
        // we don't know how many retries it will take to accomplish this query
        let sql = "SELECT SLEEP(1)";

        // retry max N times, last query should succeed
        // we use such approach as we do not know how long query will be running
        let mut results = Vec::new();
        // start retry_count from 1, to ensure it works with any retry_count as well
        for retry_count in 1_u16..20_u16 {
            // introduce delay to avoid finishing query before loop ends
            tokio::time::sleep(Duration::from_millis(100)).await;
            let result = query::<JsonResponse>(
                &query_client,
                &addr,
                &access_token,
                request_id,
                retry_count,
                sql,
                false,
            )
            .await;
            eprintln!("Retry count: {}, Result: {:?}", retry_count, result);
            if result.is_ok() {
                results.push(result);
                break;
            }
            results.push(result);
        }
        eprintln!("Final result: {:?}", results.last());
        assert!(results.len() > 1);
        assert!(results.last().unwrap().is_ok());
    }
}
