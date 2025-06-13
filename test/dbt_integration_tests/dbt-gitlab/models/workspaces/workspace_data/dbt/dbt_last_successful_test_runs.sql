WITH passing_tests AS (
    
    SELECT *
    FROM {{ ref('dbt_test_results_source') }}
    WHERE status = 'pass'
    QUALIFY row_number() OVER (PARTITION BY test_unique_id ORDER BY generated_at DESC) = 1

), failing_tests AS (

    SELECT test_unique_id
    FROM {{ ref('dbt_failing_tests') }}

), last_successful_run AS (

    SELECT *
    FROM passing_tests
    WHERE test_unique_id in (SELECT test_unique_id FROM failing_tests)

)

SELECT *
FROM last_successful_run
