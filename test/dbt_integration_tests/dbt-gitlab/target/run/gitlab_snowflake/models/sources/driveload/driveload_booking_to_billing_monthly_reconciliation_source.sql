
  
    

create or replace transient table EMBUCKET.driveload.driveload_booking_to_billing_monthly_reconciliation_source
    

    
    as (WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.booking_to_billing_monthly_reconciliation

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed
    )
;


  