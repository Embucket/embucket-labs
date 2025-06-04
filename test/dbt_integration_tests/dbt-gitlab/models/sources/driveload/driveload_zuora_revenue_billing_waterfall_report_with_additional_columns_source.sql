WITH source AS (

SELECT * 
FROM {{ source('driveload','zuora_revenue_billing_waterfall_report_with_additional_columns') }}

)

SELECT * 
FROM source
