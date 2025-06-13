{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_contact','zuora_contact_source'),
    ('zuora_account_snapshot','zuora_account_snapshots_source'),
    ('customers_billing_account_snapshot', 'customers_db_billing_accounts_snapshots_base')
]) }}

, snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), zuora_account AS (

    SELECT *
    FROM zuora_account_snapshot
    WHERE is_deleted = FALSE
      AND LOWER(live_batch) != 'batch20'

), zuora_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_account.*
    FROM zuora_account
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_account.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_account.dbt_valid_to') }}

), zuora_account_joined AS (

    SELECT
      zuora_account_spined.snapshot_id,
      zuora_account_spined.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id,
      zuora_account_spined.account_number                          AS billing_account_number,
      zuora_account_spined.account_name                            AS billing_account_name,
      zuora_account_spined.status                                  AS account_status,
      zuora_account_spined.parent_id,
      zuora_account_spined.sfdc_account_code                       AS crm_account_code,
      zuora_account_spined.sfdc_entity                             AS crm_entity,
      zuora_account_spined.currency                                AS account_currency,
      zuora_contact.country                                        AS sold_to_country,
      zuora_account_spined.ssp_channel,
      zuora_account_spined.po_required,
      zuora_account_spined.is_deleted,
      zuora_account_spined.batch,
      'Y'                                                          AS exists_in_zuora
    FROM zuora_account_spined
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account_spined.sold_to_contact_id, zuora_account_spined.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id

), cdot_billing_account_snapshot AS (

    SELECT 
      billing_account_id, 
      zuora_account_id,
      zuora_account_name,
      sfdc_account_id,
      billing_account_created_at,
      billing_account_updated_at,
      valid_from,
      valid_to
    FROM customers_billing_account_snapshot
    --Exclude Batch20(test records) from CDot by using Zuora test account IDs.
    WHERE zuora_account_id NOT IN 
      (SELECT DISTINCT 
         account_id 
       FROM zuora_account_snapshot
       WHERE LOWER(live_batch) = 'batch20'
       OR is_deleted = TRUE)

), cdot_billing_account_spined AS (

    SELECT  
      snapshot_dates.date_id AS snapshot_id,
      cdot_billing_account_snapshot.*
    FROM cdot_billing_account_snapshot
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= cdot_billing_account_snapshot.valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('cdot_billing_account_snapshot.valid_to') }}

), cdot_billing_account_joined AS (

    SELECT 
      snapshot_id,
      billing_account_id,
      map_merged_crm_account.dim_crm_account_id  AS dim_crm_account_id,
      zuora_account_id,
      zuora_account_name,
      cdot_billing_account_spined.sfdc_account_id,
      billing_account_created_at,
      billing_account_updated_at,
      valid_from,
      valid_to,
      'Y' AS exists_in_cdot
    FROM cdot_billing_account_spined
        LEFT JOIN map_merged_crm_account
      ON cdot_billing_account_spined.sfdc_account_id = map_merged_crm_account.sfdc_account_id

), joined AS (

    SELECT 
        
       --surrogate keys from zuora & cdot
      {{ dbt_utils.generate_surrogate_key(['zuora_account_joined.snapshot_id', 'zuora_account_joined.dim_billing_account_id']) }}                                                                    AS zuora_billing_account_snapshot_id,
      {{ dbt_utils.generate_surrogate_key(['cdot_billing_account_joined.snapshot_id', 'cdot_billing_account_joined.zuora_account_id']) }}                                                            AS cdot_billing_account_snapshot_id,
                                                                                           
      --snapshot keys from zuora & cdot
      zuora_account_joined.snapshot_id                                                                                                                                                      AS zuora_snapshot_id, 
      cdot_billing_account_joined.snapshot_id                                                                                                                                               AS cdot_snapshot_id,                                                                                                             
     
      --natural keys from zuora & cdot
      zuora_account_joined.dim_billing_account_id, 
      cdot_billing_account_joined.zuora_account_id,                                                                                              

      --foreign keys from zuora & cdot
      zuora_account_joined.dim_crm_account_id                                                                                                                                               AS zuora_dim_crm_account_id, 
      cdot_billing_account_joined.dim_crm_account_id                                                                                                                                        AS cdot_dim_crm_account_id,                                                                                               

      --other relevant attributes
      zuora_account_joined.billing_account_number,
      zuora_account_joined.billing_account_name, 
      cdot_billing_account_joined.zuora_account_name,                                                                                             
      zuora_account_joined.account_status,
      zuora_account_joined.parent_id,
      zuora_account_joined.crm_account_code,
      zuora_account_joined.crm_entity,
      zuora_account_joined.account_currency,
      zuora_account_joined.sold_to_country,
      zuora_account_joined.ssp_channel,
      zuora_account_joined.po_required,
      zuora_account_joined.is_deleted,
      zuora_account_joined.batch,
      CASE 
        WHEN exists_in_zuora = 'Y' and exists_in_cdot = 'Y' THEN 'exists in CDot & Zuora'
        WHEN exists_in_zuora = 'Y' and exists_in_cdot IS NULL THEN 'exists only in Zuora'
        WHEN exists_in_zuora IS NULL and exists_in_cdot = 'Y' THEN 'exists only in CDot'
      ELSE NULL 
      END                                                                                                                                                                                   AS record_data_source
    FROM zuora_account_joined
     FULL JOIN cdot_billing_account_joined
       ON zuora_account_joined.dim_billing_account_id = cdot_billing_account_joined.zuora_account_id
       AND zuora_account_joined.snapshot_id = cdot_billing_account_joined.snapshot_id

), intermediary AS  (

    SELECT
       --surrogate key
      CASE 
        WHEN joined.zuora_snapshot_id IS NOT NULL and record_data_source = 'exists only in Zuora' THEN joined.zuora_billing_account_snapshot_id 
        WHEN joined.cdot_snapshot_id IS NOT NULL and record_data_source = 'exists only in CDot' THEN joined.cdot_billing_account_snapshot_id
        WHEN joined.zuora_snapshot_id IS NOT NULL AND joined.cdot_snapshot_id IS NOT NULL and  record_data_source = 'exists in CDot & Zuora' THEN joined.zuora_billing_account_snapshot_id 
      END                                                                                                                                                                                   AS billing_account_snapshot_id,                                                                                
      
      COALESCE(joined.zuora_snapshot_id, joined.cdot_snapshot_id)                                                                                                                           AS snapshot_id,

      --natural key
      COALESCE(joined.dim_billing_account_id, joined.zuora_account_id)                                                                                                                      AS dim_billing_account_id,

      --foreign key
      COALESCE(joined.zuora_dim_crm_account_id, joined.cdot_dim_crm_account_id)                                                                                                             AS dim_crm_account_id,

      --other relevant attributes
      joined.billing_account_number,
      COALESCE(joined.billing_account_name, joined.zuora_account_name)                                                                                                                      AS billing_account_name,
      joined.account_status,
      joined.parent_id,
      joined.crm_account_code,
      joined.crm_entity,
      joined.account_currency,
      joined.sold_to_country,
      joined.ssp_channel,
      joined.po_required,
      joined.is_deleted,
      joined.batch,
      joined.record_data_source

    FROM joined

), final AS (

    SELECT 

       --surrogate key
      {{ dbt_utils.generate_surrogate_key(['intermediary.snapshot_id', 'intermediary.billing_account_snapshot_id']) }}                                                                                         AS billing_account_snapshot_id,
      intermediary.snapshot_id,

      --natural key
      intermediary.dim_billing_account_id,

      --foreign key
      intermediary.dim_crm_account_id,

      --other relevant attributes
      intermediary.billing_account_number,
      intermediary.billing_account_name,
      intermediary.account_status,
      intermediary.parent_id,
      intermediary.crm_account_code,
      intermediary.crm_entity,
      intermediary.account_currency,
      intermediary.sold_to_country,
      intermediary.ssp_channel,
      intermediary.po_required,
      intermediary.is_deleted,
      intermediary.batch,
      intermediary.record_data_source

    FROM intermediary

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@snalamaru",
    created_date="2021-08-09",
    updated_date="2023-06-06"
) }}
