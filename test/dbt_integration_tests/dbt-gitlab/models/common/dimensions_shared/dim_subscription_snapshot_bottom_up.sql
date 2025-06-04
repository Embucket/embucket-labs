{{ config({
        "tags": ["arr_snapshots", "mnpi_exception"],
        "schema": "common"
    })
}}

WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_snapshots_source') }}
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

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_snapshots_source') }}
    WHERE LOWER(subscription_status) NOT IN ('draft', 'expired')
      AND is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_subscription.*
    FROM zuora_subscription
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_subscription.dbt_valid_to') }}
    QUALIFY rank() OVER (
         PARTITION BY subscription_name, snapshot_dates.date_actual
         ORDER BY DBT_VALID_FROM DESC) = 1

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), prep_amendment AS (

  SELECT *
  FROM {{ ref('prep_amendment') }}

), subscription_lineage AS (

    SELECT DISTINCT
      subscription_name_slugify,
      subscription_lineage,
      oldest_subscription_in_cohort,
      subscription_cohort_month,
      subscription_cohort_quarter,
      subscription_cohort_year
    FROM {{ ref('map_subscription_lineage') }}

), joined AS (

    SELECT
    --Surrogate Key
      zuora_subscription_spined.snapshot_id                                     AS snapshot_id,
      zuora_subscription_spined.subscription_id                                 AS dim_subscription_id,

    --Natural Key
      zuora_subscription_spined.subscription_name                               AS subscription_name,
      zuora_subscription_spined.version                                         AS subscription_version,

    --Common Dimension Keys
      map_merged_crm_account.dim_crm_account_id                                 AS dim_crm_account_id,
      zuora_account_spined.account_id                                           AS dim_billing_account_id,
      zuora_subscription_spined.invoice_owner_id                                AS dim_billing_account_id_invoice_owner_account,
      zuora_subscription_spined.creator_account_id                              AS dim_billing_account_id_creator_account,
      zuora_subscription_spined.sfdc_opportunity_id                             AS dim_crm_opportunity_id,
      {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}                  AS dim_amendment_id_subscription,

    --Subscription Information
      zuora_subscription_spined.created_by_id,
      zuora_subscription_spined.updated_by_id,
      zuora_subscription_spined.original_id                                     AS dim_subscription_id_original,
      zuora_subscription_spined.previous_subscription_id                        AS dim_subscription_id_previous,
      zuora_subscription_spined.subscription_name_slugify,
      zuora_subscription_spined.subscription_status,
      zuora_subscription_spined.auto_renew_native_hist,
      zuora_subscription_spined.auto_renew_customerdot_hist,
      zuora_subscription_spined.zuora_renewal_subscription_name,
      zuora_subscription_spined.zuora_renewal_subscription_name_slugify,
      zuora_subscription_spined.current_term,
      zuora_subscription_spined.renewal_term,
      zuora_subscription_spined.renewal_term_period_type,
      zuora_subscription_spined.eoa_starter_bronze_offer_accepted,
      IFF(zuora_subscription_spined.created_by_id = '2c92a0fd55822b4d015593ac264767f2', -- All Self-Service / Web direct subscriptions are identified by that created_by_id
          'Self-Service', 'Sales-Assisted')                                     AS subscription_sales_type,
      invoice_owner.account_name                                                AS invoice_owner_account,
      creator_account.account_name                                              AS creator_account,
      IFF(dim_billing_account_id_invoice_owner_account != dim_billing_account_id_creator_account, TRUE, FALSE)
                                                                                AS was_purchased_through_reseller,
      zuora_subscription_spined.multi_year_deal_subscription_linkage            AS multi_year_deal_subscription_linkage,

    --Date Information
      zuora_subscription_spined.subscription_start_date                         AS subscription_start_date,
      zuora_subscription_spined.subscription_end_date                           AS subscription_end_date,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_start_date)    AS subscription_start_month,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_end_date)      AS subscription_end_month,
      snapshot_dates.fiscal_year                                                AS subscription_end_fiscal_year,
      zuora_subscription_spined.created_date::DATE                              AS subscription_created_date,
      zuora_subscription_spined.updated_date::DATE                              AS subscription_updated_date,
      zuora_subscription_spined.term_start_date::DATE                           AS term_start_date,
      zuora_subscription_spined.term_end_date::DATE                             AS term_end_date,
      DATE_TRUNC('month', zuora_subscription_spined.term_start_date::DATE)      AS term_start_month,
      DATE_TRUNC('month', zuora_subscription_spined.term_end_date::DATE)        AS term_end_month,
      CASE
        WHEN LOWER(zuora_subscription_spined.subscription_status) = 'active' AND zuora_subscription_spined.subscription_end_date > CURRENT_DATE
          THEN DATE_TRUNC('month',DATEADD('month', zuora_subscription_spined.current_term, zuora_subscription_spined.subscription_end_date::DATE))
        ELSE NULL
      END                                                                       AS second_active_renewal_month,
      zuora_subscription_spined.cancelled_date,

      --Lineage and Cohort Information
      subscription_lineage.subscription_lineage,
      subscription_lineage.oldest_subscription_in_cohort,
      subscription_lineage.subscription_cohort_month,
      subscription_lineage.subscription_cohort_quarter,
      subscription_lineage.subscription_cohort_year,

      --Supersonics Fields
      zuora_subscription_spined.turn_on_cloud_licensing,
      zuora_subscription_spined.turn_on_operational_metrics,
      zuora_subscription_spined.contract_operational_metrics,
      zuora_subscription_spined.contract_auto_renewal,
      zuora_subscription_spined.turn_on_auto_renewal,
      zuora_subscription_spined.contract_seat_reconciliation,
      zuora_subscription_spined.turn_on_seat_reconciliation
    FROM zuora_subscription_spined
    INNER JOIN zuora_account_spined
      ON zuora_subscription_spined.account_id = zuora_account_spined.account_id
      AND zuora_subscription_spined.snapshot_id = zuora_account_spined.snapshot_id
    LEFT JOIN zuora_account_spined AS invoice_owner
      ON zuora_subscription_spined.invoice_owner_id = invoice_owner.account_id
      AND zuora_subscription_spined.snapshot_id = invoice_owner.snapshot_id
    LEFT JOIN zuora_account_spined AS creator_account
      ON zuora_subscription_spined.creator_account_id = creator_account.account_id
      AND zuora_subscription_spined.snapshot_id = creator_account.snapshot_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account_spined.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN prep_amendment
      ON zuora_subscription_spined.amendment_id = prep_amendment.dim_amendment_id
    LEFT JOIN subscription_lineage
      ON subscription_lineage.subscription_name_slugify = zuora_subscription_spined.subscription_name_slugify
    LEFT JOIN snapshot_dates
      ON zuora_subscription_spined.subscription_end_date::DATE = snapshot_dates.date_day

), final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'dim_subscription_id']) }}   AS subscription_snapshot_id,
        joined.*
    FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2021-06-28",
    updated_date="2022-11-29"
) }}
