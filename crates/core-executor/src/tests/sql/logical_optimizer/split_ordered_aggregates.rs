use crate::test_query;

// Conflicting ordered aggregates should compile and return a single row
test_query!(
    split_ordered_percentiles_basic,
    "SELECT \
        percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS p50_x, \
        percentile_cont(0.5) WITHIN GROUP (ORDER BY y) AS p50_y \
     FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(x, y)",
    snapshot_path = "split_ordered_aggregates"
);

// Repro with GROUPING SETS and multiple ordered percentiles (Snowplow-like)
test_query!(
    split_ordered_grouping_sets_repro,
    "WITH src(page_url, device_class, lcp, fid) AS ( \
        SELECT '/u1', 'mobile', 120, 220 UNION ALL \
        SELECT '/u1', 'desktop', 110, 210 UNION ALL \
        SELECT '/u2', 'mobile', 130, 230 \
     ) \
     SELECT \
        page_url, \
        device_class, \
        GROUPING_ID(page_url, device_class) AS id_url_and_device, \
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lcp) AS p_lcp, \
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fid) AS p_fid \
     FROM src \
     GROUP BY GROUPING SETS ((), (page_url, device_class), (device_class)) ORDER BY page_url, device_class",
    snapshot_path = "split_ordered_aggregates"
);

// Repro with GROUPING SETS and three ordered percentiles (to expose ambiguous key issue)
test_query!(
    split_ordered_grouping_sets_three_percentiles,
    "WITH src(page_url, device_class, lcp, fid, cls) AS ( \
        SELECT '/u1', 'mobile', 120, 220, 0.10 UNION ALL \
        SELECT '/u1', 'desktop', 110, 210, 0.20 UNION ALL \
        SELECT '/u2', 'mobile', 130, 230, 0.15 \
     ) \
     SELECT \
        page_url, \
        device_class, \
        GROUPING_ID(page_url, device_class) AS id_url_and_device, \
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lcp) AS p_lcp, \
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fid) AS p_fid, \
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cls) AS p_cls \
     FROM src \
     GROUP BY GROUPING SETS ((), (page_url, device_class), (device_class)) ORDER BY page_url, device_class",
    snapshot_path = "split_ordered_aggregates"
);

// Conflicting ARRAY_AGG with more than two aggregates (no WITHIN GROUP)
test_query!(
    split_ordered_array_agg_conflicts,
    "SELECT \
        ARRAY_AGG(x ORDER BY x ASC)  AS a_asc, \
        ARRAY_AGG(x ORDER BY x DESC) AS a_desc, \
        ARRAY_AGG(x ORDER BY y ASC)  AS a_y \
     FROM (VALUES (1, 10), (2, 5), (3, 7)) AS t(x, y)",
    snapshot_path = "split_ordered_aggregates"
);

// Three percentile_cont WITHIN GROUP with conflicting ORDER BY columns
test_query!(
    split_ordered_percentile_triple,
    "SELECT \
        percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS p50_x, \
        percentile_cont(0.5) WITHIN GROUP (ORDER BY y) AS p50_y, \
        percentile_cont(0.5) WITHIN GROUP (ORDER BY z) AS p50_z \
     FROM (VALUES (1, 10, 100), (2, 20, 90), (3, 30, 80)) AS t(x, y, z)",
    snapshot_path = "split_ordered_aggregates"
);

// Mixed: three ARRAY_AGG with different ORDER BYs
test_query!(
    split_ordered_mixed_three_aggregates,
    "SELECT a, \
        ARRAY_AGG(c) WITHIN GROUP (ORDER BY c DESC) AS arr_desc, \
        ARRAY_AGG(c) WITHIN GROUP (ORDER BY d ASC)  AS arr_d, \
        ARRAY_AGG(c) WITHIN GROUP (ORDER BY a ASC)  AS arr_a \
     FROM (VALUES \
        (1, 10, 100), \
        (1, 20,  90), \
        (1, 30,  80), \
        (2,  5,  50) \
     ) AS t(a, c, d) \
     GROUP BY a ORDER BY a",
    snapshot_path = "split_ordered_aggregates"
);

test_query!(
    split_ordered_nth_value_conflicts,
    "WITH t AS (
  SELECT * FROM (
    VALUES 
      (1, 1, 10, 100), 
      (1, 1, 20,  90), 
      (1, 1, 30,  80), 
      (2, 2,  5,  50) 
  ) AS t(a, b, c, d)
)
SELECT
  a,
  b,
  NTH_VALUE(c, 2) OVER (
    PARTITION BY a, b
    ORDER BY c
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS n1,
  NTH_VALUE(c, 3) OVER (
    PARTITION BY a, b
    ORDER BY d
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS n2
FROM t
QUALIFY ROW_NUMBER() OVER (PARTITION BY a, b ORDER BY c) = 1 ORDER BY a, b;",
    snapshot_path = "split_ordered_aggregates"
);
