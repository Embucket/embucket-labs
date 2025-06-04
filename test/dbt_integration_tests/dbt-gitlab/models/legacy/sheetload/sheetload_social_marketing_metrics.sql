WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_social_marketing_metrics_source') }}

)

SELECT *
FROM source