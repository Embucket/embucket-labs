WITH source AS (

SELECT * 
FROM {{ source('driveload','zuora_revenue_waterfall_report_with_wf_type_unbilled_revenue') }}

)

SELECT * 
FROM source
