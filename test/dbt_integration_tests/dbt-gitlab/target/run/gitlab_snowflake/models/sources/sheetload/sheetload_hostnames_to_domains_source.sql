
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_hostnames_to_domains_source
    

    
    as (WITH source AS (

    SELECT * 
    FROM EMBUCKET.sheetload.hostnames_to_domains

)

SELECT * 
FROM source
    )
;


  