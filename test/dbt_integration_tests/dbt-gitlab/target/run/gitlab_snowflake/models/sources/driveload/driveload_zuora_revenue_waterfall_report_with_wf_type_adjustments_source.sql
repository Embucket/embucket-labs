
  
    

create or replace transient table EMBUCKET.driveload.driveload_zuora_revenue_waterfall_report_with_wf_type_adjustments_source
    

    
    as (WITH source AS (

SELECT * 
FROM EMBUCKET.driveload.zuora_revenue_waterfall_report_with_wf_type_adjustments

)

SELECT * 
FROM source
    )
;


  