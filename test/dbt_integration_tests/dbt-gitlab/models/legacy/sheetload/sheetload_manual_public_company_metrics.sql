WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_manual_public_company_metrics_source') }}

)

SELECT *
FROM source
