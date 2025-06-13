{{ config(
    tags=["zuora_waterfall"]
) }}

WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y_source') }}

)
SELECT * 
FROM source