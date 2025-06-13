WITH base AS (

    SELECT *
    FROM {{ ref('dbt_test_results_source') }}
    QUALIFY row_number() OVER (PARTITION BY test_unique_id ORDER BY generated_at DESC) = 1

), failures AS (

    SELECT *
    FROM base
    WHERE status != 'pass'

)

SELECT *
FROM failures
