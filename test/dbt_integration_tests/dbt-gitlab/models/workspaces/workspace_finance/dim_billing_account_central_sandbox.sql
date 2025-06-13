{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_central_sandbox_contact','zuora_central_sandbox_contact_source')
]) }}

, zuora_central_sandbox_account AS (

    SELECT *
    FROM {{ref('zuora_central_sandbox_account_source')}}
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
    WHERE LOWER(batch) != 'batch20'
      AND is_deleted = FALSE

), filtered AS (

    SELECT
      zuora_central_sandbox_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                         AS dim_crm_account_id,
      zuora_central_sandbox_account.account_number                          AS billing_account_number,
      zuora_central_sandbox_account.account_name                            AS billing_account_name,
      zuora_central_sandbox_account.status                                  AS account_status,
      zuora_central_sandbox_account.parent_id,
      zuora_central_sandbox_account.sfdc_account_code,
      zuora_central_sandbox_account.currency                                AS account_currency,
      zuora_central_sandbox_contact.country                                 AS sold_to_country,
      zuora_central_sandbox_account.is_deleted,
      zuora_central_sandbox_account.batch,
      zuora_central_sandbox_account.ssp_channel,
      zuora_central_sandbox_account.po_required
    FROM zuora_central_sandbox_account
    LEFT JOIN zuora_central_sandbox_contact
      ON COALESCE(zuora_central_sandbox_account.sold_to_contact_id, zuora_central_sandbox_account.bill_to_contact_id) = zuora_central_sandbox_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_central_sandbox_account.crm_id = map_merged_crm_account.sfdc_account_id

)

{{ dbt_audit(
    cte_ref="filtered",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-03-31",
    updated_date="2022-04-13"
) }}