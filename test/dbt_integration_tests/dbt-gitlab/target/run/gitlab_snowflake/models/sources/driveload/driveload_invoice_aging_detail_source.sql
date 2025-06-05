
  
    

create or replace transient table EMBUCKET.driveload.driveload_invoice_aging_detail_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.invoice_aging_detail

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed
    )
;


  