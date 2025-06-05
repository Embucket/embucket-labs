WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.booking_to_billing_monthly_reconciliation

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed