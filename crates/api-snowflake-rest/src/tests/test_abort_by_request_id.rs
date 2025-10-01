#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]
mod tests {
    use crate::models::{JsonResponse, LoginResponse};
    use crate::server::test_server::run_test_rest_api_server;
    use crate::tests::client::{abort, get_query_result, login, query};
    use crate::tests::sql_macro::{JSON, query_id_from_snapshot};
    use http::header;
    use uuid::Uuid;

    use axum::http;

    #[cfg(not(feature = "vanilla-tokio-runtime"))]
    #[tokio::test]
    async fn test_abort_by_request_id() {
        let addr = run_test_rest_api_server(JSON).await;
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
            query::<JsonResponse>(&client, &addr, &access_token, request_id, sql, true)
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

    #[tokio::test]
    async fn test_abort_using_wrong_request_id() {
        let addr = run_test_rest_api_server(JSON).await;
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
            query::<JsonResponse>(&client, &addr, &access_token, request_id, sql, true)
                .await
                .expect("Failed to run query");

        let _err = abort::<()>(&client, &addr, &access_token, Uuid::new_v4(), sql)
            .await
            .expect_err("abort query should fail");
    }
}
