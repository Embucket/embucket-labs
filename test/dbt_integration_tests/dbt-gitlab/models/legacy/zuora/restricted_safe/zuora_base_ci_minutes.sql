WITH invoice_details AS (

    SELECT *
    FROM {{ ref('zuora_base_invoice_details') }}
    WHERE sku = 'SKU-00000038'

)


SELECT
  account_number,
  subscription_name,
  subscription_name_slugify,
  oldest_subscription_in_cohort,
  lineage,
  cohort_month,
  cohort_quarter,
  service_month,
  charge_name,
  service_start_date,
  charge_amount,
  charge_date,
  unit_of_measure,
  unit_price,
  quantity,
  sku
FROM invoice_details
