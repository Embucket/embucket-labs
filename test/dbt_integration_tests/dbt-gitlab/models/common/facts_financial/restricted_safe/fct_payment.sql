{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain one payment, one payment may be split and applied to several invoices */

{{ simple_cte([
    ('zuora_payment_source', 'zuora_payment_source'),
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
    {{ dbt_utils.generate_surrogate_key(['zuora_payment_source.payment_id']) }}     AS payment_pk,
    
    --Natural keys 
    zuora_payment_source.payment_id,
    zuora_payment_source.payment_number,

    --Foreign keys
    zuora_account.account_id                                                        AS dim_billing_account_id,

    --Payment dates
    zuora_payment_source.payment_date,
    {{ get_date_id('zuora_payment_source.payment_date') }} AS payment_date_id,

    --Degenerative dimensions
    zuora_payment_source.payment_status,
    zuora_payment_source.payment_type,

    --Additive fields
    zuora_payment_source.payment_amount



  FROM zuora_payment_source
  INNER JOIN zuora_account
    ON zuora_payment_source.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-14"
) }}
