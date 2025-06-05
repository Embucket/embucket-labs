
  
    

create or replace transient table EMBUCKET.driveload.driveload_zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y_source
    

    
    as (WITH source AS (

SELECT * 
FROM EMBUCKET.driveload.zuora_revenue_waterfall_report_with_wf_type_net_revenue_with_mje_flag_y

)

SELECT * 
FROM source
    )
;


  