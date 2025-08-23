WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'dup_monitor_issues_and_solutions') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed