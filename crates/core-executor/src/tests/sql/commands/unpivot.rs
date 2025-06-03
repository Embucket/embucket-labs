use crate::test_query;

const SETUP_QUERY: [&str; 2] = [
    "CREATE OR REPLACE TABLE monthly_sales(
  empid INT,
  dept TEXT,
  jan INT,
  feb INT,
  mar INT,
  apr INT)",
    "INSERT INTO monthly_sales VALUES
  (1, 'electronics', 100, 200, 300, 100),
  (2, 'clothes', 100, 300, 150, 200),
  (3, 'cars', 200, 400, 100, 50),
  (4, 'appliances', 100, NULL, 100, 50);",
];

test_query!(
    unpivot_basic,
    "SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_include_nulls,
    "SELECT *
  FROM monthly_sales
    UNPIVOT INCLUDE NULLS (sales FOR month IN (jan, feb, mar, apr))
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_select_specific_columns,
    "SELECT dept, month, sales
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  ORDER BY dept;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_filtering,
    "SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  WHERE sales > 100
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_aggregation,
    "SELECT month, SUM(sales) as total_sales
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  GROUP BY month
  ORDER BY month;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_join,
    "SELECT e.empid, e.dept, u.month, u.sales
  FROM monthly_sales e
  JOIN (
    SELECT empid, month, sales
    FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  ) u ON e.empid = u.empid
  WHERE u.sales > 200
  ORDER BY e.empid, u.month;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_partial_columns,
    "SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, mar))
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_having,
    "SELECT month, SUM(sales) as total_sales
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  GROUP BY month
  HAVING SUM(sales) > 400
  ORDER BY month;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_subquery,
    "SELECT *
  FROM (
    SELECT empid, dept, jan, feb, mar
    FROM monthly_sales
    WHERE dept IN ('electronics', 'clothes')
  )
  UNPIVOT (sales FOR month IN (jan, feb, mar))
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_cte,
    "WITH sales_data AS (
  SELECT * FROM monthly_sales WHERE empid < 3
)
SELECT *
  FROM sales_data
    UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
  ORDER BY empid;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);

test_query!(
    unpivot_with_union,
    "SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (jan, feb))
  UNION ALL
SELECT *
  FROM monthly_sales
    UNPIVOT (sales FOR month IN (mar, apr))
  ORDER BY empid, month;",
    setup_queries = [SETUP_QUERY[0], SETUP_QUERY[1]],
    snapshot_path = "unpivot"
);
