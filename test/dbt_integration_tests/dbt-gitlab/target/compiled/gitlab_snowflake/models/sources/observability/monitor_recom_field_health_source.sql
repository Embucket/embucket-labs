WITH source AS (

    SELECT *
    FROM MONTE_CARLO.prod_insights.monitor_recom_field_health

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed