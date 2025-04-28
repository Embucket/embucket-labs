use crate::tests::utils::macros::test_query;

test_query!(select_date_add_diff, "SELECT dateadd(day, 5, '2025-06-01')");
// // SELECT
// test_query!(select_star, "SELECT * FROM employee_table");
// test_query!(select_ilike, "SELECT * ILIKE '%id%' FROM employee_table;");
// test_query!(select_func, "SELECT my_func(a, b, c) FROM employee_table;");
// test_query!(func_date_add, "SELECT date_add(day, 30, '2025-01-06')");
// test_query!(
//     select_exclude,
//     "SELECT * EXCLUDE department_id FROM employee_table;"
// );
// test_query!(
//     select_exclude_multiple,
//     "SELECT * EXCLUDE (department_id, employee_id) FROM employee_table;"
// );
// test_query!(
//     select_all_rename,
//     "SELECT * RENAME (department_id AS department, employee_id AS id) FROM employee_table;"
// );
// test_query!(select_all_exclude_rename, "SELECT * EXCLUDE first_name RENAME (department_id AS department, employee_id AS id) FROM employee_table;");
// test_query!(
//     select_ilike_rename,
//     "SELECT * ILIKE '%id%' RENAME department_id AS department FROM employee_table;"
// );
// test_query!(
//     select_rename,
//     "SELECT * RENAME department_id AS department FROM employee_table;"
// );
// test_query!(
//     select_all_replace_value,
//     "SELECT * REPLACE ('DEPT-' || department_id AS department_id) FROM employee_table;"
// );
// test_query!(select_all_replace_rename, "SELECT * REPLACE ('DEPT-' || department_id AS department_id) RENAME department_id AS department FROM employee_table;");
// test_query!(
//     select_ilike_replace_value,
//     "SELECT * ILIKE '%id%' REPLACE('DEPT-' || department_id AS department_id) FROM employee_table;"
// );
// test_query!(
//     select_all_exclude_rename_join,
//     "SELECT
//   employee_table.* EXCLUDE department_id,
//   department_table.* RENAME department_name AS department
// FROM employee_table INNER JOIN department_table
//   ON employee_table.department_id = department_table.department_id
// ORDER BY department, last_name, first_name;"
// );
// test_query!(
//     select_by_position,
//     "SELECT $2 FROM employee_table ORDER BY $2;"
// );
// test_query!(select_as, "SELECT pi() * 2.0 * 2.0 AS area_of_circle;");
// test_query!(select_top, "select TOP 4 c1 from testtable;");
// test_query!(
//     select_values,
//     "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three'));"
// );

// // FROM
// test_query!(
//     from_inline_view,
//     "SELECT v.profit
//     FROM (SELECT retail_price - wholesale_cost AS profit FROM ftable1) AS v;"
// );
// test_query!(
//     from_sample,
//     "SELECT *
//     FROM quarterly_sales SAMPLE(10);"
// );
// test_query!(
//     from_tablesample,
//     "SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3);"
// );
// test_query!(
//     from_tablesample_all,
//     "SELECT * FROM testtable TABLESAMPLE (100);"
// );
// test_query!(from_emptysample, "SELECT * FROM testtable SAMPLE ROW (0);");
// test_query!(
//     from_sample_seed,
//     "SELECT * FROM testtable SAMPLE SYSTEM (3) SEED (82);"
// );
// test_query!(
//     from_sample_block,
//     "SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992);"
// );
// test_query!(
//     from_sample_fixed,
//     "SELECT * FROM testtable SAMPLE (10 ROWS);"
// );

// test_query!(
//     from_udtf,
//     "SELECT *
//     FROM TABLE(Fibonacci_Sequence_UDTF(6.0::FLOAT));"
// );
// test_query!(
//     from_at,
//     "SELECT *
//     FROM quarterly_sales AT(OFFSET => -86400);"
// );
// test_query!(
//     from_at_timestamp,
//     "SELECT * FROM tt1 at(TIMESTAMP => '2024-06-05 15:29:00'::TIMESTAMP_LTZ);"
// );

// // JOIN
// test_query!(
//     join_inner,
//     "SELECT t1.col1, t2.col1
//     FROM t1 INNER JOIN t2
//         ON t2.col1 = t1.col1
//     ORDER BY 1,2;"
// );
// test_query!(
//     join_left_outer,
//     "SELECT t1.col1, t2.col1
//     FROM t1 LEFT OUTER JOIN t2
//         ON t2.col1 = t1.col1
//     ORDER BY 1,2;"
// );
// test_query!(
//     join_right_outer,
//     "SELECT t1.col1, t2.col1
//     FROM t1 RIGHT OUTER JOIN t2
//         ON t2.col1 = t1.col1
//     ORDER BY 1,2;"
// );
// test_query!(
//     join_full_outer,
//     "SELECT t1.col1, t2.col1
//     FROM t1 FULL OUTER JOIN t2
//         ON t2.col1 = t1.col1
//     ORDER BY 1,2;"
// );
// test_query!(
//     join_cross,
//     "SELECT t1.col1, t2.col1
//     FROM t1 CROSS JOIN t2
//     ORDER BY 1, 2;"
// );
// test_query!(
//     join_cross_where,
//     "SELECT t1.col1, t2.col1
//     FROM t1 CROSS JOIN t2
//     WHERE t2.col1 = t1.col1
//     ORDER BY 1, 2;"
// );
// test_query!(
//     join_natural_inner,
//     "SELECT *
//     FROM d1 NATURAL INNER JOIN d2
//     ORDER BY id;"
// );
// test_query!(
//     join_natural_full_outer,
//     "SELECT *
//   FROM d1 NATURAL FULL OUTER JOIN d2
//   ORDER BY ID;"
// );
// test_query!(
//     join_with_as,
//     "WITH
//     l AS (
//          SELECT 'a' AS userid
//          ),
//     r AS (
//          SELECT 'b' AS userid
//          )
//   SELECT *
//     FROM l LEFT JOIN r USING(userid)
// ;"
// );
// test_query!(
//     join_outer_join_operator,
//     "SELECT d.department_name, p.project_name, e.employee_name
//     FROM  departments d, projects p, employees e
//     WHERE
//             p.department_id = d.department_id
//         AND
//             e.project_id(+) = p.project_id
//     ORDER BY d.department_id, p.project_id, e.employee_id;"
// );

// // LATERAL
// test_query!(lateral_subquery, "SELECT *
//     FROM departments AS d, LATERAL (SELECT * FROM employees AS e WHERE e.department_ID = d.department_ID) AS iv2
//     ORDER BY employee_ID;");
// test_query!(lateral_inner_join, "SELECT *
//     FROM departments AS d INNER JOIN LATERAL (SELECT * FROM employees AS e WHERE e.department_ID = d.department_ID) AS iv2
//     ORDER BY employee_ID;");

// //PIVOT
// test_query!(
//     pivot_column_values,
//     "SELECT *
//   FROM quarterly_sales
//     PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
//   ORDER BY empid;"
// );
// test_query!(
//     pivot_subquery,
//     "SELECT *
//   FROM quarterly_sales
//     PIVOT(SUM(amount) FOR quarter IN (
//       SELECT DISTINCT quarter
//         FROM ad_campaign_types_by_quarter
//         WHERE television = TRUE
//         ORDER BY quarter))
//   ORDER BY empid;"
// );
// test_query!(
//     pivot_dynamic,
//     "WITH
//   src AS
//   (
//     SELECT *
//       FROM quarterly_sales
//         PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
//   )
// SELECT em.managerid, src.*
//   FROM emp_manager em
//   JOIN src ON em.empid = src.empid
//   ORDER BY empid;"
// );

// //UNPIVOT
// test_query!(
//     unpivot,
//     "SELECT *
//   FROM monthly_sales
//     UNPIVOT (sales FOR month IN (jan, feb, mar, apr))
//   ORDER BY empid;"
// );
// test_query!(
//     unpivot_include_nulls,
//     "SELECT *
//   FROM monthly_sales
//     UNPIVOT INCLUDE NULLS (sales FOR month IN (jan, feb, mar, apr))
//   ORDER BY empid;"
// );

// // WHERE
// test_query!(
//     where_simple,
//     "SELECT * FROM employee_table WHERE department_id = 1;"
// );
// test_query!(
//     where_subquery,
//     "SELECT * FROM sales
//     WHERE retail_price < (
//                    SELECT AVG(retail_price)
//                        FROM sales
//                    )
//     ;"
// );

// // GROUP BY
// test_query!(
//     group_by_simple,
//     "SELECT product_ID, SUM(retail_price * quantity) AS gross_revenue
//   FROM sales
//   GROUP BY product_ID;"
// );
// test_query!(
//     group_by_multiple,
//     "SELECT state, city, SUM(retail_price * quantity) AS gross_revenue
//   FROM sales
//   GROUP BY state, city;"
// );
// test_query!(
//     group_by_all,
//     "SELECT state, city, SUM(retail_price * quantity) AS gross_revenue
//   FROM sales
//   GROUP BY ALL;"
// );
// test_query!(
//     group_by_cube,
//     "SELECT state, city, SUM((s.retail_price - p.wholesale_price) * s.quantity) AS profit
//  FROM products AS p, sales AS s
//  WHERE s.product_ID = p.product_ID
//  GROUP BY CUBE (state, city)
//  ORDER BY state, city NULLS LAST
//  ;"
// );
// test_query!(
//     group_by_grouping_sets,
//     "SELECT COUNT(*), medical_license, radio_license
//   FROM nurses
//   GROUP BY GROUPING SETS (medical_license, radio_license);"
// );
// test_query!(
//     group_by_rollup,
//     "SELECT state, city, SUM((s.retail_price - p.wholesale_price) * s.quantity) AS profit
//  FROM products AS p, sales AS s
//  WHERE s.product_ID = p.product_ID
//  GROUP BY ROLLUP (state, city)
//  ORDER BY state, city NULLS LAST
//  ;"
// );
// test_query!(
//     group_by_having,
//     "SELECT department_id
// FROM employees
// GROUP BY department_id
// HAVING count(*) < 10;"
// );

// // QUALIFY

// test_query!(
//     qualify,
//     "SELECT product_id, retail_price, quantity, city
//     FROM sales
//     QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY retail_price) = 1
//     ;"
// );

// // ORDER BY

// test_query!(order_by, "SELECT * FROM sales ORDER BY product_id");

// // LIMIT OFFSET

// test_query!(
//     limit_offset,
//     "select retail_price from sales limit 3 offset 3;"
// );

// // CASTING

// test_query!(
//     cast_simple,
//     "SELECT col1::NUMBER(6,5) AS varchar_to_number2
//   FROM t1;"
// );
// test_query!(try_cast, "SELECT TRY_CAST('05-Mar-2016' AS TIMESTAMP);");
// test_query!(
//     union_explicit_cast,
//     "SELECT col1::VARCHAR FROM t1
// UNION
// SELECT col1::VARCHAR FROM t2;"
// );

// // DESCRIBE

// test_query!(describe_database, "DESCRIBE DATABASE desc_demo;");
// test_query!(
//     describe_warehouse,
//     "DESCRIBE WAREHOUSE temporary_warehouse;"
// );
// test_query!(describe_table, "DESCRIBE TABLE departments;");

// // VARIABLES

// test_query!(show_variables, "SHOW VARIABLES");
// test_query!(show_variables_like, "SHOW VARIABLES LIKE '%TESTING%'");

// test_query!(show_warehouses, "SHOW WAREHOUSES");
// test_query!(show_warehouses_like, "SHOW WAREHOUSES LIKE '%TESTING%'");

// test_query!(show_databases, "SHOW DATABASES");
// test_query!(show_databases_like, "SHOW DATABASES LIKE '%TESTING%'");

// test_query!(show_tables, "SHOW TABLES");
// test_query!(show_tables_like, "SHOW TABLES LIKE '%TESTING%'");

// test_query!(show_views, "SHOW VIEWS");
// test_query!(show_views_like, "SHOW VIEWS LIKE '%TESTING%'");

// // ICEBERG

// test_query!(
//     create_iceberg_table,
//     "CREATE ICEBERG TABLE my_delta_iceberg_table
//   CATALOG = delta_catalog_integration
//   EXTERNAL_VOLUME = delta_external_volume
//   BASE_LOCATION = 'relative/path/from/ext/vol/';"
// );

// // DELETE

// test_query!(
//     delete_from,
//     "DELETE FROM leased_bicycles WHERE bicycle_ID = 105;"
// );

// // UPDATE

// test_query!(
//     update_row,
//     "UPDATE leased_bicycles SET customer_id = 1234 WHERE bicycle_id=101;"
// );

// // MERGE
// test_query!(
//     merge_into,
//     "MERGE INTO target_table USING source_table
//     ON target_table.id = source_table.id
//     WHEN MATCHED THEN
//         UPDATE SET target_table.description = source_table.description;"
// );

// test_query!(
//     merge_into_matched,
//     "MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
//     WHEN MATCHED AND t2.marked = 1 THEN DELETE
//     WHEN MATCHED AND t2.isNewStatus = 1 THEN UPDATE SET val = t2.newVal, status = t2.newStatus
//     WHEN MATCHED THEN UPDATE SET val = t2.newVal
//     WHEN NOT MATCHED THEN INSERT (val, status) VALUES (t2.newVal, t2.newStatus);"
// );

// // COLUMN TYPES

// /*test_query!(type_variant, r#"CREATE OR REPLACE TABLE variant_insert (v VARIANT);
// INSERT INTO variant_insert (v)
//   SELECT PARSE_JSON('{"key3": "value3", "key4": "value4"}');"#);*/
// /*test_query!(type_object, "CREATE OR REPLACE TABLE object_example (object_column OBJECT);
// INSERT INTO object_example (object_column)
//   SELECT OBJECT_CONSTRUCT('thirteen', 13::VARIANT, 'zero', 0::VARIANT);");*/
// test_query!(
//     type_cast_array_to_array,
//     "SELECT CAST(
//   CAST([1,2,3] AS ARRAY(NUMBER))
//   AS ARRAY(VARCHAR)) AS cast_array;"
// );
// test_query!(
//     type_change_key_names,
//     "SELECT CAST({'city':'San Mateo','state': 'CA'}::OBJECT(city VARCHAR, state VARCHAR)
//   AS OBJECT(city_name VARCHAR, state_name VARCHAR) RENAME FIELDS) AS object_value_key_names;"
// );
// test_query!(
//     type_array_construct,
//     "SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(NUMBER);"
// );
// test_query!(
//     type_object_construct,
//     "SELECT OBJECT_CONSTRUCT(
//   'oname', 'abc',
//   'created_date', '2020-01-18'::DATE
// )::OBJECT(
//   oname VARCHAR,
//   created_date DATE
// );"
// );
// test_query!(
//     type_map_from_object,
//     "SELECT OBJECT_CONSTRUCT(
//   'city', 'San Mateo',
//   'state', 'CA'
// )::MAP(
//   VARCHAR,
//   VARCHAR
// );"
// );
// test_query!(
//     type_map,
//     "SELECT {
//   'city': 'San Mateo',
//   'state': 'CA'
// }::MAP(
//   VARCHAR,
//   VARCHAR
// );"
// );
// test_query!(
//     type_object_keys,
//     "SELECT OBJECT_KEYS({'city':'San Mateo','state':'CA'}::OBJECT(city VARCHAR, state VARCHAR));"
// );
// test_query!(
//     type_map_keys,
//     "SELECT MAP_KEYS({'my_key':'my_value'}::MAP(VARCHAR,VARCHAR));"
// );
// test_query!(
//     type_array_size,
//     "SELECT ARRAY_SIZE([1,2,3]::ARRAY(NUMBER));"
// );
// test_query!(
//     type_map_size,
//     "SELECT MAP_SIZE({'my_key':'my_value'}::MAP(VARCHAR,VARCHAR));"
// );
// test_query!(
//     type_array_contains,
//     "SELECT ARRAY_CONTAINS(10, [1, 10, 100]::ARRAY(NUMBER));"
// );
// test_query!(
//     type_array_position,
//     "SELECT ARRAY_POSITION(10, [1, 10, 100]::ARRAY(NUMBER));"
// );
// test_query!(
//     type_map_contains_key,
//     "SELECT MAP_CONTAINS_KEY('key_to_find', {'my_key':'my_value'}::MAP(VARCHAR,VARCHAR));"
// );
