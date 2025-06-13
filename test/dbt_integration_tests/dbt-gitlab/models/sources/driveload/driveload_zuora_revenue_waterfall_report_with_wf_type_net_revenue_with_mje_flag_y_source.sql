WITH source AS (

SELECT * 
FROM {{ source('driveload','zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y') }}

)

SELECT * 
FROM source
