WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'dup_monitor_recom_field_health') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed