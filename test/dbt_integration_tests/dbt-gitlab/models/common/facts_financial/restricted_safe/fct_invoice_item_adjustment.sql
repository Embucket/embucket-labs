{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: lowest grain is an invoice item adjustment made on the invoice */

{{ simple_cte([
    ('zuora_invoice_item_adjustment_source', 'zuora_invoice_item_adjustment_source'),
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
    {{ dbt_utils.generate_surrogate_key(['zuora_invoice_item_adjustment_source.invoice_item_adjustment_id']) }} AS invoice_item_adjustment_pk,

    --Natural keys 
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_id,
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_number,

    --Foreign keys
    zuora_account.account_id                                                                                                                     AS dim_billing_account_id,
    zuora_invoice_item_adjustment_source.invoice_id                                                                                              AS dim_invoice_id,
    zuora_invoice_item_adjustment_source.accounting_period_id,


    --Invoice item adjustment dates
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_date,
    {{ get_date_id('zuora_invoice_item_adjustment_source.invoice_item_adjustment_date') }} AS invoice_item_adjustment_date_id,

    --Degenerative fields
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_status,
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_type,

    --Additive fields
    zuora_invoice_item_adjustment_source.invoice_item_adjustment_amount


  FROM zuora_invoice_item_adjustment_source
  INNER JOIN zuora_account
    ON zuora_invoice_item_adjustment_source.account_id = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-14"
) }}
