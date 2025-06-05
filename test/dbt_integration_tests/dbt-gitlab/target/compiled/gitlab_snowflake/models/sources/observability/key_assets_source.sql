WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.key_assets

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed