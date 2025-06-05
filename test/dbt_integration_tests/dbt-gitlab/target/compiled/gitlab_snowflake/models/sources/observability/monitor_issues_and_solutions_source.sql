WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.monitor_issues_and_solutions

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed