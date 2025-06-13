WITH crm_account_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_account')}}

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_source') }}
    WHERE is_deleted = FALSE

), final_invoice AS (

    SELECT
    --ids
      zuora_invoice.invoice_id                                          AS dim_invoice_id,

      --shared dimension keys
      zuora_invoice.account_id                                          AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                        AS dim_crm_account_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      crm_account_dimensions.dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      crm_account_dimensions.dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id,

      --invoice dates
      {{ get_date_id('zuora_invoice.invoice_date') }}                   AS invoice_date_id,
      {{ get_date_id('zuora_invoice.created_date') }}                   AS created_date_id,
      {{ get_date_id('zuora_invoice.due_date') }}                       AS due_date_id,
      {{ get_date_id('zuora_invoice.posted_date') }}                    AS posted_date_id,
      {{ get_date_id('zuora_invoice.target_date') }}                    AS target_date_id,

      --invoice flags
      zuora_invoice.includes_one_time,
      zuora_invoice.includesrecurring,
      zuora_invoice.includes_usage,
      zuora_invoice.transferred_to_accounting,

      --additive fields
      zuora_invoice.adjustment_amount,
      zuora_invoice.amount,
      zuora_invoice.amount_without_tax,
      zuora_invoice.balance,
      zuora_invoice.credit_balance_adjustment_amount,
      zuora_invoice.payment_amount,
      zuora_invoice.refund_amount,
      zuora_invoice.tax_amount,
      zuora_invoice.tax_exempt_amount,

      -- metadata
      zuora_invoice.created_by_id,
      zuora_invoice.updated_by_id,
      {{ get_date_id('zuora_invoice.updated_date') }}                   AS updated_date_id

    FROM zuora_invoice
    INNER JOIN zuora_account
      ON zuora_invoice.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN crm_account_dimensions
      ON map_merged_crm_account.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
)

{{ dbt_audit(
    cte_ref="final_invoice",
    created_by="@mcooperDD",
    updated_by="@lisvinueza",
    created_date="2021-01-20",
    updated_date="2023-05-21"
) }}
