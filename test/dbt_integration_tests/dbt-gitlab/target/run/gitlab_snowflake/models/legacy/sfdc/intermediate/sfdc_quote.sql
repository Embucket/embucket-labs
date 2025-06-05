
  
    

create or replace transient table EMBUCKET.legacy.sfdc_quote
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sfdc.sfdc_quote_source
    WHERE is_deleted = FALSE
)

SELECT *
FROM source
    )
;


  