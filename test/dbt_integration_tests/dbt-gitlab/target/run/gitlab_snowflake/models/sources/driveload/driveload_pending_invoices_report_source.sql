
  
    

create or replace transient table EMBUCKET.driveload.driveload_pending_invoices_report_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.pending_invoices_report

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed
    )
;


  