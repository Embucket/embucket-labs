use crate::test_query;

test_query!(
    result_scan_last_query_id,
    "SELECT * FROM result_scan(last_query_id())"
);
test_query!(
    result_scan_last_query_id_index,
    "SELECT * FROM result_scan(last_query_id(-1))"
);
test_query!(result_scan_query_id, "SELECT * FROM result_scan('1')");
test_query!(
    result_scan_query_id_filter,
    "SELECT * FROM result_scan('1') WHERE a > 1"
);
// Missing query
test_query!(
    result_scan_missing_query_id,
    "SELECT * FROM result_scan('500')"
);
// Failed query with error
test_query!(result_scan_failed_query, "SELECT * FROM result_scan('100')");
// Invalid id type
test_query!(
    result_scan_invalid_query_id,
    "SELECT * FROM result_scan('aa')"
);
