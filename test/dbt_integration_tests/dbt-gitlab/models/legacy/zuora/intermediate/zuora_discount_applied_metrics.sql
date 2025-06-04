WITH source AS (

    SELECT *
    FROM {{ ref('zuora_discount_applied_metrics_source') }}

)

SELECT *
FROM source
