use crate::visitors::{
    fetch_to_limit, functions_rewriter, inline_aliases_in_query, json_element, like_ilike_any,
    rlike_regexp_expr_rewriter, select_expr_aliases, table_functions, table_functions_cte_relation,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_common::Result as DFResult;

#[test]
fn test_like_ilike_any_expr_rewriter() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        //LIKE ANY
        (
            "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%','T%e')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 LIKE '%Jo%oe%' OR column1 LIKE 'T%e' ORDER BY column1",
        ),
        (
            "SELECT *
             FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%','T%e', '%Tim%')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 LIKE '%Jo%oe%' OR column1 LIKE 'T%e' OR column1 LIKE '%Tim%' ORDER BY column1",
        ),
        (
            "SELECT *
             FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 LIKE ANY ('%Jo%oe%')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 LIKE '%Jo%oe%' ORDER BY column1",
        ),
        //ILIKE ANY
        (
            "SELECT * FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 ILIKE ANY ('%Jo%oe%','T%e')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 ILIKE '%Jo%oe%' OR column1 ILIKE 'T%e' ORDER BY column1",
        ),
        (
            "SELECT *
             FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 ILIKE ANY ('%Jo%oe%','T%e', '%Tim%')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 ILIKE '%Jo%oe%' OR column1 ILIKE 'T%e' OR column1 ILIKE '%Tim%' ORDER BY column1",
        ),
        (
            "SELECT *
             FROM VALUES
                ('John  Dddoe'),
                ('Joe   Doe'),
                ('John_down'),
                ('Joe down'),
                ('Tom   Doe'),
                ('Tim down'),
                (NULL)
            WHERE column1 ILIKE ANY ('%Jo%oe%')
            ORDER BY column1",
            "SELECT * FROM (VALUES ('John  Dddoe'), ('Joe   Doe'), ('John_down'), ('Joe down'), ('Tom   Doe'), ('Tim down'), (NULL)) WHERE column1 ILIKE '%Jo%oe%' ORDER BY column1",
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            like_ilike_any::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_rlike_regexp_expr_rewriter() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        (
            "SELECT 'nevermore' RLIKE 'never'",
            "SELECT regexp_like('nevermore', 'never')",
        ),
        (
            "SELECT 'nevermore' REGEXP 'never'",
            "SELECT regexp_like('nevermore', 'never')",
        ),
        (
            "SELECT 'nevermore' NOT RLIKE 'never'",
            "SELECT NOT regexp_like('nevermore', 'never')",
        ),
        (
            "SELECT 'nevermore' NOT REGEXP 'never'",
            "SELECT NOT regexp_like('nevermore', 'never')",
        ),
        //the `values` will be put inside the `()`, not by the rewriter
        (
            "SELECT column1 FROM VALUES ('San Francisco'), ('San Jose'), ('Santa Clara'), ('Sacramento') WHERE column1 RLIKE 'San* [fF].*'",
            "SELECT column1 FROM (VALUES ('San Francisco'), ('San Jose'), ('Santa Clara'), ('Sacramento')) WHERE regexp_like(column1, 'San* [fF].*')",
        ),
        (
            "SELECT column1 FROM VALUES ('San Francisco'), ('San Jose'), ('Santa Clara'), ('Sacramento') WHERE column1 NOT RLIKE 'San* [fF].*'",
            "SELECT column1 FROM (VALUES ('San Francisco'), ('San Jose'), ('Santa Clara'), ('Sacramento')) WHERE NOT regexp_like(column1, 'San* [fF].*')",
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            rlike_regexp_expr_rewriter::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_json_element() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        (
            "SELECT context[0]::varchar",
            "SELECT json_get(context, 0)::VARCHAR",
        ),
        (
            "SELECT context[0]:id::varchar",
            "SELECT json_get(json_get(context, 0), 'id')::VARCHAR",
        ),
        (
            "SELECT context[0].id::varchar",
            "SELECT json_get(json_get(context, 0), 'id')::VARCHAR",
        ),
        (
            "SELECT context[0]:id[1]::varchar",
            "SELECT json_get(json_get(json_get(context, 0), 'id'), 1)::VARCHAR",
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            json_element::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_functions_rewriter() -> DFResult<()> {
    let state = SessionContext::new().state();

    let cases = vec![
        // timestamp keywords postprocess
        (
            "SELECT dateadd(year, 5, '2025-06-01')",
            "SELECT dateadd('year', 5, '2025-06-01')",
        ),
        (
            "SELECT dateadd(\"year\", 5, '2025-06-01')",
            "SELECT dateadd('year', 5, '2025-06-01')",
        ),
        (
            "SELECT dateadd('year', 5, '2025-06-01')",
            "SELECT dateadd('year', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(day, 5, '2025-06-01')",
            "SELECT datediff('day', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(week, 5, '2025-06-01')",
            "SELECT datediff('week', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(nsecond, 10000000, '2025-06-01')",
            "SELECT datediff('nsecond', 10000000, '2025-06-01')",
        ),
        (
            "SELECT date_diff(hour, 5, '2025-06-01')",
            "SELECT date_diff('hour', 5, '2025-06-01')",
        ),
        (
            "SELECT date_add(us, 100000, '2025-06-01')",
            "SELECT date_add('us', 100000, '2025-06-01')",
        ),
        (
            "SELECT date_part(year, TO_TIMESTAMP('2024-04-08T23:39:20.123-07:00'))",
            "SELECT date_part('year', to_timestamp('2024-04-08T23:39:20.123-07:00'))",
        ),
        (
            "SELECT date_part(epoch_second, TO_TIMESTAMP('2024-04-08T23:39:20.123-07:00'))",
            "SELECT date_part('epoch', to_timestamp('2024-04-08T23:39:20.123-07:00'))",
        ),
        (
            "SELECT date_part(epoch_seconds, TO_TIMESTAMP('2024-04-08T23:39:20.123-07:00'))",
            "SELECT date_part('epoch', to_timestamp('2024-04-08T23:39:20.123-07:00'))",
        ),
        (
            "SELECT date_part('epoch_seconds', TO_TIMESTAMP('2024-04-08T23:39:20.123-07:00'))",
            "SELECT date_part('epoch', to_timestamp('2024-04-08T23:39:20.123-07:00'))",
        ),
        (
            "SELECT date_part(\"epoch_second\", TO_TIMESTAMP('2024-04-08T23:39:20.123-07:00'))",
            "SELECT date_part('epoch', to_timestamp('2024-04-08T23:39:20.123-07:00'))",
        ),
        // to_char format replacements
        (
            "SELECT to_char(col::DATE, 'YYYYMMDD')",
            "SELECT to_char(col::DATE, '%Y%m%d')",
        ),
        (
            "SELECT to_char(col::DATE, 'DD-MM-YYYY')",
            "SELECT to_char(col::DATE, '%d-%m-%Y')",
        ),
        (
            "SELECT to_char(col::DATE, 'MM/DD/YYYY HH24:MI:SS')",
            "SELECT to_char(col::DATE, '%m/%d/%Y %H:%M:%S')",
        ),
        (
            "SELECT to_char(col::DATE, 'YYYY/MM/DD HH24')",
            "SELECT to_char(col::DATE, '%Y/%m/%d %H')",
        ),
        ("SELECT grouping_id(a)", "SELECT grouping(a)"),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            functions_rewriter::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_select_expr_aliases() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        // Unique expression names
        (
            "SELECT to_date('2024-05-10'), to_date('2024-05-10')",
            "SELECT to_date('2024-05-10'), to_date('2024-05-10') AS expr_0",
        ),
        // Unique expression names with existing aliases
        (
            "SELECT TO_DATE('2024-05-10') AS dt, TO_DATE('2024-05-10') AS dt2",
            "SELECT TO_DATE('2024-05-10') AS dt, TO_DATE('2024-05-10') AS dt2",
        ),
        // Unique expression names with some aliases
        (
            "SELECT TO_DATE('2024-05-10') AS dt, TO_DATE('2024-05-10')",
            "SELECT TO_DATE('2024-05-10') AS dt, TO_DATE('2024-05-10')",
        ),
        // Unique expression names nested select
        (
            "SELECT (SELECT TO_DATE('2024-05-10'), TO_DATE('2024-05-10'))",
            "SELECT (SELECT TO_DATE('2024-05-10'), TO_DATE('2024-05-10') AS expr_0)",
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            select_expr_aliases::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_inline_aliases_in_query() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        (
            "SELECT 'test txt' AS alias, length(alias) AS t",
            "SELECT 'test txt' AS alias, length('test txt') AS t",
        ),
        (
            "SELECT 1 + 2 AS sum, sum + 3 AS total",
            "SELECT 1 + 2 AS sum, 1 + 2 + 3 AS total",
        ),
        (
            "SELECT 10 AS val, val + 5 AS res",
            "SELECT 10 AS val, 10 + 5 AS res",
        ),
        (
            "SELECT 1 AS val, (SELECT val + 1) AS subquery",
            "SELECT 1 AS val, (SELECT 1 + 1) AS subquery",
        ),
        (
            "WITH cte AS (SELECT 1 AS one, one + 1 AS two) SELECT two FROM cte",
            "WITH cte AS (SELECT 1 AS one, 1 + 1 AS two) SELECT two FROM cte",
        ),
        (
            "WITH snowplow_events_sample AS (
                SELECT 'd1' AS domain_userid, 'user_a' AS user_id, CAST('2023-10-25 10:00:00' AS TIMESTAMP) AS collector_tstamp
                UNION ALL
                SELECT 'd1', 'user_b', CAST('2023-10-25 12:30:00' AS TIMESTAMP)
            )
            SELECT DISTINCT
                domain_userid,
                last_value(user_id) OVER (
                    PARTITION BY domain_userid
                    ORDER BY collector_tstamp
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS user_id
            FROM snowplow_events_sample;",
            "WITH snowplow_events_sample AS \
            (SELECT 'd1' AS domain_userid, 'user_a' AS user_id, CAST('2023-10-25 10:00:00' AS TIMESTAMP) AS collector_tstamp UNION ALL \
            SELECT 'd1', 'user_b', CAST('2023-10-25 12:30:00' AS TIMESTAMP)) \
            SELECT DISTINCT domain_userid, last_value(user_id) \
            OVER (PARTITION BY domain_userid ORDER BY collector_tstamp \
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
            AS user_id FROM snowplow_events_sample",
        ),
        (
            "select  sum(page_views) as page_views,  sum(engaged_time_in_s) as engaged_time_in_s from  test group by 1,2",
            "SELECT sum(page_views) AS page_views, sum(engaged_time_in_s) AS engaged_time_in_s FROM test GROUP BY 1, 2"
        ),
        (
            "with test as (select 122 as b) SELECT b as c FROM test QUALIFY ROW_NUMBER() OVER(PARTITION BY c) = 1",
            "WITH test AS (SELECT 122 AS b) SELECT b AS c FROM test QUALIFY ROW_NUMBER() OVER (PARTITION BY b) = 1"
        ),
        (
            "SELECT 123 AS a WHERE a > 100",
            "SELECT 123 AS a WHERE 123 > 100"
        ),
        (
            "WITH data_points AS (
                SELECT
                  SPLIT_PART(metric_name, '.', 14)::VARCHAR AS aggregation_name,
                  SPLIT_PART(metric_name, '.', 6)::VARCHAR AS metric_name
                FROM some_table
            )
            SELECT * FROM data_points",
            "WITH data_points AS (SELECT SPLIT_PART(metric_name, '.', 14)::VARCHAR AS aggregation_name, SPLIT_PART(metric_name, '.', 6)::VARCHAR AS metric_name FROM some_table) SELECT * FROM data_points"
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            inline_aliases_in_query::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_table_function_result_scan() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![
        (
            "SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID(-2)))",
            "SELECT * FROM RESULT_SCAN(LAST_QUERY_ID(-2))",
        ),
        (
            "SELECT * FROM table(FUNC('1'))",
            "SELECT * FROM TABLE(FUNC('1'))",
        ),
        (
            "SELECT c2 FROM TABLE(RESULT_SCAN('id')) WHERE c2 > 1",
            "SELECT c2 FROM RESULT_SCAN('id') WHERE c2 > 1",
        ),
        (
            "select a.*, b.IS_ICEBERG as 'is_iceberg'
            from table(result_scan(last_query_id(-1))) a left join test as b on a.t = b.t",
            "SELECT a.*, b.IS_ICEBERG AS 'is_iceberg' FROM result_scan(last_query_id(-1)) AS a LEFT JOIN test AS b ON a.t = b.t",
        ),
        (
            "SELECT * FROM TABLE(FLATTEN(input => parse_json('[1, 77]')))",
            "SELECT * FROM FLATTEN(input => parse_json('[1, 77]'))",
        ),
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            table_functions::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}

#[test]
fn test_fetch_to_limit_error_on_missing_quantity() -> DFResult<()> {
    let state = SessionContext::new().state();
    let sql = "SELECT * FROM test FETCH FIRST ROWS ONLY";
    let mut statement = state.sql_to_statement(sql, "snowflake")?;

    if let DFStatement::Statement(ref mut stmt) = statement {
        let result = fetch_to_limit::visit(stmt);
        match result {
            Err(error) => {
                let error_msg = error.to_string();
                assert!(error_msg.contains("FETCH requires a quantity to be specified"));
            }
            Ok(()) => panic!("Expected error for FETCH without quantity"),
        }
    }

    Ok(())
}

#[test]
fn test_table_function_cte() -> DFResult<()> {
    let state = SessionContext::new().state();
    let cases = vec![(
        r#"WITH base AS (SELECT '{"a": 1}' AS jsontext),
            intermediate AS (
              SELECT value
              FROM base, LATERAL FLATTEN(INPUT => parse_json(jsontext)) d
            )
            SELECT * FROM intermediate;"#,
        "WITH base AS (SELECT '{\"a\": 1}' AS jsontext), intermediate AS \
           (SELECT value FROM base, LATERAL FLATTEN(INPUT => parse_json((SELECT jsontext FROM \
           (SELECT '{\"a\": 1}' AS jsontext) AS base))) AS d) SELECT * FROM intermediate",
    )];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            table_functions_cte_relation::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}
