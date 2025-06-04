WITH source AS (

    SELECT *
    FROM {{ ref('monitor_recom_field_health_source') }}

)

SELECT *
FROM source
