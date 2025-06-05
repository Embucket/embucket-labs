
  
    

create or replace transient table EMBUCKET.legacy.netsuite_accounts
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.netsuite.netsuite_accounts_source

)

SELECT *
FROM source
    )
;


  