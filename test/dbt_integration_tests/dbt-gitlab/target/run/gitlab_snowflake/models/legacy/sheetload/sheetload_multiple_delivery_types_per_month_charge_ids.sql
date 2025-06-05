
  
    

create or replace transient table EMBUCKET.legacy.sheetload_multiple_delivery_types_per_month_charge_ids
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_multiple_delivery_types_per_month_charge_ids_source

)

SELECT *
FROM source
    )
;


  