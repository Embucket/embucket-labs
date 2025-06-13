-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ ref('zuora_refund_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
  AND account_id NOT IN ({{ zuora_excluded_accounts() }})
