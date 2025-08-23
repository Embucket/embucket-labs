// Aggregate function tests for GROUPING_ID

use crate::test_query;

test_query!(
    grouping_id,
    "SELECT k, GROUPING_ID(k) AS gid, SUM(v) AS s FROM (VALUES (1,10),(1,20)) AS t(k,v) GROUP BY GROUPING SETS ((k), ()) ORDER BY gid, k",
    snapshot_path = "aggregate"
);
