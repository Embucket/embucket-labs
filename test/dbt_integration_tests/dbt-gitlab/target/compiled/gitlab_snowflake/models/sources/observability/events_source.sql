WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.events

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed