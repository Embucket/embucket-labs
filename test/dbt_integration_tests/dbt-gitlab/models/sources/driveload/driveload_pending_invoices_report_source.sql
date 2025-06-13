WITH source AS (

  SELECT * 
  FROM {{ source('driveload', 'pending_invoices_report') }}

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed