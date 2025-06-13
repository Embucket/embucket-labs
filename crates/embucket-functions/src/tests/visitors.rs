use crate::visitors::{
    functions_rewriter, inline_aliases_in_query, json_element, select_expr_aliases,
    table_result_scan,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion_common::Result as DFResult;

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
        ("select year(ts)", "SELECT date_part('year', ts)"),
        ("select dayofyear(ts)", "SELECT date_part('doy', ts)"),
        ("select day(ts)", "SELECT date_part('day', ts)"),
        ("select dayofmonth(ts)", "SELECT date_part('day', ts)"),
        ("select dayofweek(ts)", "SELECT date_part('dow', ts)"),
        ("select month(ts)", "SELECT date_part('month', ts)"),
        ("select weekofyear(ts)", "SELECT date_part('week', ts)"),
        ("select week(ts)", "SELECT date_part('week', ts)"),
        ("select hour(ts)", "SELECT date_part('hour', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        ("select second(ts)", "SELECT date_part('second', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        ("select yearofweek(ts)", "SELECT yearofweek(ts)"),
        ("select yearofweekiso(ts)", "SELECT yearofweekiso(ts)"),
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
        )
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
    ];

    for (input, expected) in cases {
        let mut statement = state.sql_to_statement(input, "snowflake")?;
        if let DFStatement::Statement(ref mut stmt) = statement {
            table_result_scan::visit(stmt);
        }
        assert_eq!(statement.to_string(), expected);
    }
    Ok(())
}
