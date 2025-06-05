
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.driveload_zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y
    

    
    as (

WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.driveload_zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y_source

)
SELECT * 
FROM source
    )
;


  