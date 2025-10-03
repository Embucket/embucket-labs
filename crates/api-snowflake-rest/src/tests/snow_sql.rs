use super::client::{get_query_result, login, query};
use crate::models::{JsonResponse, LoginResponse};
use http::header;
use std::net::SocketAddr;
use uuid::Uuid;

pub async fn snow_sql(server_addr: &SocketAddr, user: &str, pass: &str, sql: &str) -> JsonResponse {
    // introduce 2ms (to be sure) delay every time running query via "snow sql" as an issue workaround:
    // https://github.com/Embucket/embucket/issues/1630
    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;

    let client = reqwest::Client::new();
    let (headers, login_res) = login::<LoginResponse>(&client, server_addr, user, pass)
        .await
        .expect("Failed to login");
    assert_eq!(headers.get(header::WWW_AUTHENTICATE), None);

    let access_token = login_res
        .data
        .clone()
        .map_or_else(String::new, |data| data.token);

    if sql.starts_with("!result") {
        let query_id = sql.trim_start_matches("!result ");

        let (_headers, history_res) =
            get_query_result::<JsonResponse>(&client, server_addr, &access_token, query_id)
                .await
                .expect("Failed to get query result");
        history_res
    } else {
        // if sql ends with ;> it is async query
        let (sql, async_exec) = if sql.ends_with(";>") {
            (sql.trim_end_matches(";>"), true)
        } else {
            (sql, false)
        };

        let sql = if sql.starts_with("!abort") {
            let query_id = sql.trim_start_matches("!abort ");
            &format!("SELECT SYSTEM$CANCEL_QUERY('{query_id}');")
        } else {
            sql
        };

        let request_id = Uuid::new_v4();
        let (_headers, res) = query::<JsonResponse>(
            &client,
            server_addr,
            &access_token,
            request_id,
            1,
            sql,
            async_exec,
        )
        .await
        .expect("Failed to run query");
        res
    }
}
