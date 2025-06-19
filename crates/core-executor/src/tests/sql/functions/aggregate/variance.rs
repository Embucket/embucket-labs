// Aggregate function tests for VARIANCE and VARIANCE_SAMP

use crate::test_query;

test_query!(
    variance_aggregate,
    "SELECT VARIANCE(n) AS v FROM (VALUES (1), (2), (3), (4)) AS t(n)",
    snapshot_path = "aggregate"
);

test_query!(
    variance_samp_aggregate,
    "SELECT VARIANCE_SAMP(n) AS v FROM (VALUES (1), (2), (3), (4)) AS t(n)",
    snapshot_path = "aggregate"
);

test_query!(
    variance_window,
    "SELECT VARIANCE(n) OVER () AS v FROM (VALUES (1), (2), (3), (4)) AS t(n)",
    snapshot_path = "aggregate"
);

test_query!(
    variance_samp_window,
    "SELECT VARIANCE_SAMP(n) OVER () AS v FROM (VALUES (1), (2), (3), (4)) AS t(n)",
    snapshot_path = "aggregate"
);
