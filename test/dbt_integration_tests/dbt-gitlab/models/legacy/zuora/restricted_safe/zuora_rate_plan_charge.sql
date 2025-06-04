WITH source AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
  AND account_id NOT IN ({{ zuora_excluded_accounts() }})
