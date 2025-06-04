WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_booking_to_billing_monthly_reconciliation_source') }}

)
SELECT * 
FROM source