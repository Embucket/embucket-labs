
  
    

create or replace transient table EMBUCKET.legacy.sfdc_record_type
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sfdc.sfdc_record_type_source

)

SELECT *
FROM source
    )
;


  