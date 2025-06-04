WITH source AS (

  SELECT * 
  FROM {{ source('driveload', 'invoice_aging_detail') }}

), renamed AS (

    SELECT
      *
    FROM source

)

SELECT * 
FROM renamed