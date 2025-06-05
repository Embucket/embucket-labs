
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_license_md5_to_subscription_mapping_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.license_md5_to_subscription_mapping

)

SELECT *
FROM source
    )
;


  