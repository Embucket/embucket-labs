WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.incident_history

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed