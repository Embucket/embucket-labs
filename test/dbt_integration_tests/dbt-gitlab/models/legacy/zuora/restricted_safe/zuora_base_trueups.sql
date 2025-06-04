WITH invoice_details AS (

  SELECT *
  FROM {{ ref('zuora_base_invoice_details') }}
  WHERE LOWER(charge_name) LIKE '%trueup%'

)

, final AS (
  
  SELECT
    -- PRIMARY KEY
    subscription_name_slugify,
    
    -- LOGICAL INFO
    account_number,
    charge_name,
    subscription_name,
    
    -- LINEAGE
    lineage,
    oldest_subscription_in_cohort,
    
    -- METADATA
    cohort_month,
    cohort_quarter,
    country,
    service_month                 AS trueup_month,
    service_start_date,
    
    -- REVENUE DATA
    charge_amount,
    charge_amount/12              AS mrr,
    unit_of_measure,
    unit_price

  FROM invoice_details

)

SELECT * 
FROM final
