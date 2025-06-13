{{ config({
        "tags": ["mnpi_exception"],
    })
}}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_contact','zuora_contact_source'),
    ('zuora_payment_method', 'zuora_payment_method_source'),
    ('customers_billing_account','customers_db_billing_accounts_source')
]) }}

, zuora_account AS (

    SELECT *
    FROM {{ref('zuora_account_source')}}
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
    WHERE LOWER(batch) != 'batch20'
      AND is_deleted = FALSE

), zuora_billing_account AS (

    SELECT
      zuora_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id             AS dim_crm_account_id,
      zuora_account.account_number                          AS billing_account_number,
      zuora_account.account_name                            AS billing_account_name,
      zuora_account.status                                  AS account_status,
      zuora_account.parent_id,
      zuora_account.sfdc_account_code,
      zuora_account.sfdc_entity,
      zuora_account.currency                                AS account_currency,
      zuora_contact.country                                 AS sold_to_country,
      zuora_account.ssp_channel,
      CASE
        WHEN zuora_account.po_required = '' THEN 'NO'
        WHEN zuora_account.po_required IS NULL THEN 'NO'
        ELSE zuora_account.po_required
      END                                                   AS po_required,
      zuora_account.auto_pay,
      zuora_payment_method.payment_method_type              AS default_payment_method_type,
      zuora_account.is_deleted,
      zuora_account.batch,
      'Y' as exists_in_zuora
    FROM zuora_account
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN zuora_payment_method
      ON zuora_account.default_payment_method_id = zuora_payment_method.payment_method_id

), cdot_billing_account AS (

    SELECT 
      billing_account_id,
      map_merged_crm_account.dim_crm_account_id             AS dim_crm_account_id,
      zuora_account_id,
      zuora_account_name,
      customers_billing_account.sfdc_account_id,
      billing_account_created_at,
      billing_account_updated_at,
      'Y' as exists_in_cdot
    FROM customers_billing_account
    LEFT JOIN map_merged_crm_account
      ON customers_billing_account.sfdc_account_id = map_merged_crm_account.sfdc_account_id
    --Exclude Batch20(test records) from CDot by using Zuora test account IDs.
    WHERE zuora_account_id NOT IN 
      (SELECT DISTINCT 
        account_id 
       FROM {{ref('zuora_account_source')}}
       WHERE LOWER(batch) = 'batch20'
       OR is_deleted = TRUE)

), final AS (

    SELECT 
      --surrogate key
      {{ dbt_utils.generate_surrogate_key(['COALESCE(zuora_billing_account.dim_billing_account_id, cdot_billing_account.zuora_account_id)']) }}  AS dim_billing_account_sk,

      --natural key
      COALESCE(zuora_billing_account.dim_billing_account_id, cdot_billing_account.zuora_account_id)                                     AS dim_billing_account_id,

      --foreign key
      COALESCE(zuora_billing_account.dim_crm_account_id, cdot_billing_account.dim_crm_account_id)                                       AS dim_crm_account_id,

      --other relevant attributes
      zuora_billing_account.billing_account_number,
      COALESCE(zuora_billing_account.billing_account_name, cdot_billing_account.zuora_account_name)                                     AS billing_account_name,
      zuora_billing_account.account_status,
      zuora_billing_account.parent_id,
      zuora_billing_account.sfdc_account_code,
      zuora_billing_account.sfdc_entity,
      zuora_billing_account.account_currency,
      zuora_billing_account.sold_to_country,
      zuora_billing_account.ssp_channel,
      zuora_billing_account.po_required,
      zuora_billing_account.auto_pay,
      zuora_billing_account.default_payment_method_type,
      zuora_billing_account.is_deleted,
      zuora_billing_account.batch,
      CASE 
            WHEN exists_in_zuora = 'Y' and exists_in_cdot = 'Y' THEN 'exists in CDot & Zuora'
            WHEN exists_in_zuora = 'Y' and exists_in_cdot IS NULL THEN 'exists only in Zuora'
            WHEN exists_in_zuora IS NULL and exists_in_cdot = 'Y' THEN 'exists only in CDot'
            ELSE NULL 
      END                                                                                                                               AS record_data_source
      FROM zuora_billing_account 
    FULL JOIN cdot_billing_account 
      ON zuora_billing_account.dim_billing_account_id = cdot_billing_account.zuora_account_id
) 


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-04-24",
    updated_date="2023-06-14"
) }}