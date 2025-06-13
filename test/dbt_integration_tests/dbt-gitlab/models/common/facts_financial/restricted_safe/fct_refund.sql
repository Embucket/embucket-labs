{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is a refund made out off an invoice or account */

{{ simple_cte([
    ('zuora_refund_source', 'zuora_refund_source'),
    ('zuora_account_source', 'zuora_account_source')
]) }},

zuora_account AS (

  SELECT *
  FROM zuora_account_source
  WHERE is_deleted = FALSE

),

final AS (

  SELECT
    --Primary key
    {{ dbt_utils.generate_surrogate_key(['zuora_refund_source.refund_id']) }} AS refund_pk,

    --Natural key 
    zuora_refund_source.refund_id,
    zuora_refund_source.refund_number,

    --Foreign keys
    zuora_account.account_id                                                  AS dim_billing_account_id,
  
    --Refund dates
    zuora_refund_source.refund_date,
    {{ get_date_id('zuora_refund_source.refund_date') }} AS refund_date_id,

    --Degenerative dimensions
    zuora_refund_source.refund_status,
    zuora_refund_source.refund_type,

    --Additive fields
    zuora_refund_source.refund_amount



  FROM zuora_refund_source
  INNER JOIN zuora_account
    ON zuora_refund_source.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-14"
) }}
