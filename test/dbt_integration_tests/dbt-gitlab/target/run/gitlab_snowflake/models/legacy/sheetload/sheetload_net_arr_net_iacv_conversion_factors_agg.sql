
  
    

create or replace transient table EMBUCKET.legacy.sheetload_net_arr_net_iacv_conversion_factors_agg
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_net_arr_net_iacv_conversion_factors_agg_source

)

SELECT *
FROM source
    )
;


  