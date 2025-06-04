WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_invoice_aging_detail_source') }}

)
SELECT * 
FROM source