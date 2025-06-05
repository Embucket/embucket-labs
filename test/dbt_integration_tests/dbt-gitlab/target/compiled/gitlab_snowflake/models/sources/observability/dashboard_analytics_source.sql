WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.dashboard_analytics

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed