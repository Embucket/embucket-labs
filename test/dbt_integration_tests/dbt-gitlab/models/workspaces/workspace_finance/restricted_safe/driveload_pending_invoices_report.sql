WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_pending_invoices_report_source') }}

)
SELECT * 
FROM source