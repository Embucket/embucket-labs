
  
    

create or replace transient table EMBUCKET.restricted_safe_legacy.sheetload_net_arr_net_iacv_conversion_factors
    

    
    as (

WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_net_arr_net_iacv_conversion_factors_source

)

SELECT *
FROM source
    )
;


  