
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_multiple_delivery_types_per_month_charge_ids_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.multiple_delivery_types_per_month_charge_ids

)

SELECT *
FROM source
    )
;


  