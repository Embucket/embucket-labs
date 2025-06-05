WITH source AS (

  SELECT * 
  FROM EMBUCKET.driveload.pending_invoices_report

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed