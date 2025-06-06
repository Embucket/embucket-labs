use crate::test_query;

const SETUP_QUERY: &str = "CREATE OR REPLACE TABLE quarterly_sales(
  empid INT,
  amount INT,
  quarter TEXT)
  AS SELECT * FROM VALUES
    (1, 10000, '2023_Q1'),
    (1, 400, '2023_Q1'),
    (2, 4500, '2023_Q1'),
    (2, 35000, '2023_Q1'),
    (1, 5000, '2023_Q2'),
    (1, 3000, '2023_Q2'),
    (2, 200, '2023_Q2'),
    (2, 90500, '2023_Q2'),
    (1, 6000, '2023_Q3'),
    (1, 5000, '2023_Q3'),
    (2, 2500, '2023_Q3'),
    (2, 9500, '2023_Q3'),
    (3, 2700, '2023_Q3'),
    (1, 8000, '2023_Q4'),
    (1, 10000, '2023_Q4'),
    (2, 800, '2023_Q4'),
    (2, 4500, '2023_Q4'),
    (3, 2700, '2023_Q4'),
    (3, 16000, '2023_Q4'),
    (3, 10200, '2023_Q4')";

test_query!(
    pivot_basic,
    "SELECT *
FROM quarterly_sales
PIVOT(SUM(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q3', '2023_Q4'))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_null_handling,
    "SELECT *
FROM quarterly_sales
PIVOT(SUM(amount) 
  FOR quarter IN ('2023_Q1', '2023_Q2')
  DEFAULT ON NULL (1001))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_any_automatic_detection,
    "SELECT *
FROM quarterly_sales
PIVOT(SUM(amount) FOR empid IN (ANY ORDER BY empid))
ORDER BY quarter;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_any_with_output_reordering,
    "SELECT *
FROM quarterly_sales
PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter DESC))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_subquery,
    "SELECT *
FROM quarterly_sales
PIVOT(SUM(amount) 
  FOR quarter IN (
    SELECT DISTINCT quarter FROM quarterly_sales WHERE quarter LIKE '%Q1' OR quarter LIKE '%Q3'
  ))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_cte,
    "WITH sales_without_discount AS
  (SELECT empid, amount, quarter FROM quarterly_sales)
SELECT *
FROM sales_without_discount
PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_filtered_subquery,
    "SELECT *
FROM (
  SELECT empid, amount, quarter 
  FROM quarterly_sales 
  WHERE amount > 5000
)
PIVOT(SUM(amount) FOR quarter IN ('2023_Q1', '2023_Q4'))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_case_expressions,
    "SELECT *
FROM (
  SELECT 
    empid,
    amount,
    CASE
      WHEN quarter IN ('2023_Q1', '2023_Q2') THEN 'H1'
      WHEN quarter IN ('2023_Q3', '2023_Q4') THEN 'H2'
    END AS half_year
  FROM quarterly_sales
)
PIVOT(SUM(amount) FOR half_year IN ('H1', 'H2'))
ORDER BY empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_union,
    "SELECT 'Average sale amount' AS aggregate, *
  FROM quarterly_sales
    PIVOT(AVG(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q4'))
UNION
SELECT 'Highest value sale' AS aggregate, *
  FROM quarterly_sales
    PIVOT(MAX(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q4'))
UNION
SELECT 'Lowest value sale' AS aggregate, *
  FROM quarterly_sales
    PIVOT(MIN(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q4'))
UNION
SELECT 'Number of sales' AS aggregate, *
  FROM quarterly_sales
    PIVOT(COUNT(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q4'))
UNION
SELECT 'Total amount' AS aggregate, *
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN ('2023_Q1', '2023_Q2', '2023_Q4'))
ORDER BY aggregate, empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_union_with_any,
    "SELECT 'Average sale amount' AS aggregate, *
  FROM quarterly_sales
    PIVOT(AVG(amount) FOR quarter IN (ANY ORDER BY quarter))
UNION
SELECT 'Highest value sale' AS aggregate, *
  FROM quarterly_sales
    PIVOT(MAX(amount) FOR quarter IN (ANY ORDER BY quarter))
UNION
SELECT 'Lowest value sale' AS aggregate, *
  FROM quarterly_sales
    PIVOT(MIN(amount) FOR quarter IN (ANY ORDER BY quarter))
UNION
SELECT 'Number of sales' AS aggregate, *
  FROM quarterly_sales
    PIVOT(COUNT(amount) FOR quarter IN (ANY ORDER BY quarter))
UNION
SELECT 'Total amount' AS aggregate, *
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
ORDER BY aggregate, empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);

test_query!(
    pivot_with_avg_any,
    "SELECT 'Average sale amount' AS aggregate, *
 FROM quarterly_sales
   PIVOT(AVG(amount) FOR quarter IN (ANY ORDER BY quarter)) ORDER by empid;",
    setup_queries = [SETUP_QUERY],
    snapshot_path = "pivot"
);
