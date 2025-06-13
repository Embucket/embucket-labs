{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_account_source', 'zuora_account_source'),
    ('zuora_contact_source', 'zuora_contact_source'),
    ('zuora_query_api_users_source', 'zuora_query_api_users_source')
]) }}

, zuora_account AS (

    SELECT zuora_account_source.*
    FROM zuora_account_source
    LEFT JOIN zuora_contact_source AS bill_to_contact
      ON zuora_account_source.bill_to_contact_id = bill_to_contact.contact_id
    LEFT JOIN zuora_contact_source AS sold_to_contact
      ON sold_to_contact_id = sold_to_contact.contact_id
    LEFT JOIN zuora_query_api_users_source
      ON zuora_account_source.updated_by_id = zuora_query_api_users_source.zuora_user_id
    WHERE -- filters to remove known data quality issues based on feedback from Enterprise Apps
      LOWER(zuora_account_source.batch) != 'batch20'
      AND zuora_account_source.is_deleted = FALSE
      AND zuora_account_source.status != 'Canceled'
      AND (LOWER(bill_to_contact.work_email) NOT LIKE '%@gitlab.com%' AND LOWER(sold_to_contact.work_email) NOT LIKE '%@gitlab.com%')
      AND COALESCE(bill_to_contact.work_email, bill_to_contact.personal_email, sold_to_contact.work_email, sold_to_contact.personal_email) IS NOT NULL
      AND COALESCE(bill_to_contact.work_email, bill_to_contact.personal_email, sold_to_contact.work_email, sold_to_contact.personal_email) != '' -- sometimes these values look null, but are actually blank spaces
      AND zuora_query_api_users_source.email != 'svc_zuora_fulfillment_int@gitlab.com'

), final AS (

    SELECT
      zuora_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id             AS dim_crm_account_id_merged,
      zuora_account.crm_id                                  AS dim_crm_account_id_zuora,
      zuora_account.account_number                          AS billing_account_number
    FROM zuora_account
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    WHERE dim_crm_account_id_merged != dim_crm_account_id_zuora
      OR dim_crm_account_id_zuora IS NULL
      OR dim_crm_account_id_merged IS NULL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-10-07",
    updated_date="2023-05-01"
) }}