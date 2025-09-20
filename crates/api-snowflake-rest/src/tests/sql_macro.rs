use crate::models::JsonResponse;

pub const DEMO_USER: &str = "embucket";
pub const DEMO_PASSWORD: &str = "embucket";

#[must_use]
pub fn insta_replace_filiters() -> Vec<(&'static str, &'static str)> {
    vec![(
        r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
        "UUID",
    )]
}

pub fn query_id_from_snapshot(
    snapshot: &JsonResponse,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    if let Some(data) = &snapshot.data {
        if let Some(query_id) = &data.query_id {
            Ok(query_id.clone())
        } else {
            Err("No query ID".into())
        }
    } else {
        Err("No data".into())
    }
}

#[derive(Debug)]
pub struct HistoricalCodes {
    pub sql_state: String,
    pub error_code: String,
}

impl std::fmt::Display for HistoricalCodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sqlState={}; errorCode={};",
            self.sql_state, self.error_code
        )
    }
}

#[macro_export]
macro_rules! sql_test {
    ($name:ident, $sqls:expr) => {
        #[tokio::test]
        async fn $name() {
            use $crate::tests::snow_sql::{SnowSqlCommand, snow_sql};
            use $crate::models::JsonResponse;
            use $crate::tests::sql_macro::{DEMO_PASSWORD, DEMO_USER,
                insta_replace_filiters,
                query_id_from_snapshot,
            };

            let mod_name = module_path!().split("::").last().unwrap();
            let server_addr = run_test_rest_api_server().await;
            let mut prev_response: Option<JsonResponse> = None;
            let test_start = std::time::Instant::now();
            for (idx, sql) in $sqls.iter().enumerate() {
                let idx = idx + 1;
                let mut sql = sql.to_string();
                let sql_start = std::time::Instant::now();

                // replace $LAST_QUERY_ID by query_id from previous response
                if sql.contains("$LAST_QUERY_ID") {
                    let resp = prev_response.expect("No previous response");
                    let last_query_id = query_id_from_snapshot(&resp).expect("Can't acquire value for $LAST_QUERY_ID");
                    sql = sql.replace("$LAST_QUERY_ID", &last_query_id);
                }

                let snapshot = snow_sql(&server_addr, DEMO_USER, DEMO_PASSWORD, SnowSqlCommand::Query(sql.to_string())).await;
                let test_duration = test_start.elapsed().as_millis();
                let sql_duration = sql_start.elapsed().as_millis();
                let async_query = sql.ends_with(";>").then(|| "Async ").unwrap_or("");
                let sql_info = format!("{async_query}SQL #{idx} [spent: {sql_duration}/{test_duration}ms]: {sql}");

                println!("{sql_info}");
                insta::with_settings!({
                    snapshot_path => format!("snapshots/{mod_name}"),
                    // for debug purposes fetch query_id of current query
                    description => format!("{sql_info}\nQuery UUID: {}",
                        query_id_from_snapshot(&snapshot)
                            .map_or_else(|_| "No query ID".to_string(), |id| id)),
                    sort_maps => true,
                    filters => insta_replace_filiters(),
                    // info => &info,
                }, {
                    let pretty_json = serde_json::to_string_pretty(&snapshot)
                        .expect("serialize snapshot");
                    insta::assert_snapshot!(pretty_json);
                });

                prev_response = Some(snapshot);
            }
        }
    };
}
