#![allow(clippy::expect_used, clippy::unwrap_used)]
use crate::entities::query::{QueryRecord, QueryStatus};
use crate::entities::worksheet::Worksheet;
use crate::interface::GetQueriesParams;
use crate::*;
use crate::{QueryRecordId, QueryResultError};
use chrono::{Duration, TimeZone, Utc};
use tokio;

fn create_query_records(templates: &[(Option<i64>, QueryStatus)]) -> Vec<QueryRecord> {
    let mut created: Vec<QueryRecord> = vec![];
    for (i, (worksheet_id, query_status)) in templates.iter().enumerate() {
        let query_record_fn = |query: &str, worksheet_id: Option<WorksheetId>| -> QueryRecord {
            let start_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
                + Duration::microseconds(i.try_into().expect("Failed convert idx to microseconds"));
            let mut record = QueryRecord::new(query, worksheet_id);
            record.id = QueryRecordId(start_time.timestamp_micros());
            record.start_time = start_time;
            record.status = QueryStatus::Running;
            record
        };
        let query_record = match query_status {
            QueryStatus::Running => query_record_fn(format!("select {i}").as_str(), *worksheet_id),
            QueryStatus::Successful => {
                let mut item = query_record_fn(format!("select {i}").as_str(), *worksheet_id);
                item.finished_with_status(*query_status, 1);
                item
            }
            QueryStatus::Canceled | QueryStatus::TimedOut | QueryStatus::Failed => {
                let mut item = query_record_fn(format!("select {i}").as_str(), *worksheet_id);
                item.finished_with_error(&QueryResultError {
                    status: *query_status,
                    message: String::from("Test query pseudo error"),
                    diagnostic_message: String::from("diagnostic message"),
                });
                item
            }
        };
        created.push(query_record);
    }

    created
}

#[tokio::test]
async fn test_history() {
    let db = HistoryStoreDb::new_in_memory().await;

    // create a worksheet first
    let worksheet = Worksheet::new(String::new(), String::new());
    let worksheet = db
        .add_worksheet(worksheet)
        .await
        .expect("Failed creating worksheet");

    let mut created = create_query_records(&[
        (Some(worksheet.id), QueryStatus::Successful),
        (Some(worksheet.id), QueryStatus::Failed),
        (Some(worksheet.id), QueryStatus::TimedOut),
        (None, QueryStatus::Canceled),
        (None, QueryStatus::Running),
    ]);
    created.sort_by(|i1, i2| i2.id.as_i64().cmp(&i1.id.as_i64()));

    for item in &created {
        db.add_query(item).await.expect("Failed adding query");
        // update result set for successful queries (separate update + insert)
        if item.status == QueryStatus::Successful {
            let test_result_set = ResultSet {
                columns: vec![Column {
                    name: "Int64(2)".to_string(),
                    r#type: "fixed".to_string(),
                }],
                rows: serde_json::from_str("[[2]]").unwrap(),
                data_format: "json".to_string(),
                schema: "schema".to_string(),
                id: item.id,
                batch_size_bytes: 0,
                configured_rows_limit: None,
            };
            db.update_query(item, Some(test_result_set))
                .await
                .expect("Failed updating query");
        }
    }

    let get_queries_params = GetQueriesParams::new()
        .with_worksheet_id(worksheet.id)
        .with_limit(10);
    let retrieved_worksheet_queries = db
        .get_queries(get_queries_params)
        .await
        .expect("Failed getting queries");
    // queries belong to the worksheet
    assert_eq!(3, retrieved_worksheet_queries.len());

    let get_queries_params = GetQueriesParams::new().with_limit(10);
    let retrieved_all = db
        .get_queries(get_queries_params)
        .await
        .expect("Failed getting queries");
    // all queries
    for item in &retrieved_all {
        eprintln!("retrieved_all : {item:?}");
    }
    assert_eq!(created.len(), retrieved_all.len());
    assert_eq!(created, retrieved_all);

    // Delete worksheet & check related keys
    db.delete_worksheet(worksheet.id)
        .await
        .expect("Failed deleting worksheet");
    let worksheet_queries = db
        .get_queries(GetQueriesParams::default().with_worksheet_id(worksheet.id))
        .await
        .expect("Failed getting worksheet's queries");
    if !worksheet_queries.is_empty() {
        eprintln!("rudiment key left after worksheet deleted: {worksheet_queries:?}");
    }
    assert_eq!(worksheet_queries.len(), 0);
}
