WITH test_results AS (

    SELECT *
    FROM {{ ref('dbt_test_results_source') }}

), tests AS (

    SELECT *
    FROM {{ ref('dbt_test_source') }}

), joined AS (

    SELECT
      test_results.test_execution_time_elapsed,
      test_results.test_unique_id,
      test_results.status                        AS test_status,
      test_results.message                       AS test_message,
      test_results.generated_at                  AS results_generated_at,
      tests.name                                 AS test_name,
      tests.alias                                AS test_alias,
      tests.test_type,
      tests.package_name,
      tests.tags                                 AS test_tags,
      tests.severity                             AS test_severity,
      tests.referrences                          AS test_references
    FROM test_results
    INNER JOIN tests
      ON test_results.test_unique_id = tests.unique_id

)

SELECT *
FROM joined
