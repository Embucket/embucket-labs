WITH source AS (

    SELECT *
    FROM {{ ref('dashboard_analytics_source') }}

)

SELECT *
FROM source
