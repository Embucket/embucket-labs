
  
    

create or replace transient table EMBUCKET.legacy.sheetload_license_md5_to_subscription_mapping
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.sheetload_license_md5_to_subscription_mapping_source

)

SELECT *
FROM source
    )
;


  