{{ config({
    "tags": ["mnpi_exception"],
    "alias": "dim_subscription"
}) }}

WITH prep_amendment AS (

  SELECT *
  FROM {{ ref('prep_amendment') }}

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), subscription_opportunity_mapping AS (

    SELECT *
    FROM {{ ref('map_subscription_opportunity') }}

), subscription_lineage AS (

    SELECT DISTINCT
      subscription_name_slugify,
      subscription_lineage,
      oldest_subscription_in_cohort,
      subscription_cohort_month,
      subscription_cohort_quarter,
      subscription_cohort_year
    FROM {{ ref('map_subscription_lineage') }}

), data_quality_filter_subscription_slugify AS (
    
    /*
    There was a data quality issue where a subscription_name_slugify can be mapped to more than one subscription_name. 
    There are 5 subscription_name_slugifys and 10 subscription_names that this impacts as of 2023-02-20. This CTE is 
    used to filter out these subscriptions from the model. The data quality issue causes a fanout with the subscription 
    lineages that are used to group on in the data model.
    This DQ issue has been fixed and the way subscriptions are named now does not have this problem.
    So this CTE is for cleaning up historical data and future proofing the model in case this DQ issue come again.
    */

    SELECT 
      subscription_name_slugify,
      1 AS is_data_quality_filter_subscription_slugify_flag,
      COUNT(subscription_name) AS nbr_records
    FROM subscription
    WHERE subscription_status IN ('Active', 'Cancelled')
    GROUP BY 1
    HAVING nbr_records > 1

), joined AS (

  SELECT
    --Surrogate Key
    subscription.dim_subscription_id,

    --Natural Key
    subscription.subscription_name,
    subscription.subscription_version,

    --Common Dimension Keys
    subscription.dim_crm_account_id,
    subscription.dim_billing_account_id,
    subscription.dim_billing_account_id_invoice_owner_account,
    subscription.dim_billing_account_id_creator_account,
    CASE
       WHEN subscription.subscription_created_date < '2019-02-01'
         THEN NULL
       ELSE subscription_opportunity_mapping.dim_crm_opportunity_id
    END                                                                             AS dim_crm_opportunity_id,
    subscription.dim_crm_opportunity_id_current_open_renewal,
    subscription.dim_crm_opportunity_id_closed_lost_renewal,
    {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}                        AS dim_amendment_id_subscription,

    --Subscription Information
    subscription.created_by_id,
    subscription.updated_by_id,
    subscription.dim_subscription_id_original,
    subscription.dim_subscription_id_previous,
    subscription.subscription_name_slugify,
    subscription.subscription_status,
    subscription.namespace_id,
    subscription.namespace_name,
    subscription.zuora_renewal_subscription_name,
    subscription.zuora_renewal_subscription_name_slugify,
    subscription.current_term,
    subscription.renewal_term,
    subscription.renewal_term_period_type,
    subscription.eoa_starter_bronze_offer_accepted,
    subscription.subscription_sales_type,
    subscription.auto_renew_native_hist,
    subscription.auto_renew_customerdot_hist,
    subscription.turn_on_cloud_licensing,
    subscription.turn_on_operational_metrics,
    subscription.contract_operational_metrics,
    subscription.contract_auto_renewal,
    subscription.turn_on_auto_renewal,
    subscription.contract_seat_reconciliation,
    subscription.turn_on_seat_reconciliation,
    subscription_opportunity_mapping.is_questionable_opportunity_mapping,
    subscription.invoice_owner_account,
    subscription.creator_account,
    subscription.was_purchased_through_reseller,
    subscription.multi_year_deal_subscription_linkage,
    subscription.ramp_id,
    subscription.is_ramp,
    COALESCE(dqf.is_data_quality_filter_subscription_slugify_flag, 0)               AS is_data_quality_filter_subscription_slugify_flag,

    --Date Information
    subscription.subscription_start_date,
    subscription.subscription_end_date,
    subscription.subscription_start_month,
    subscription.subscription_end_month,
    subscription.subscription_end_fiscal_year,
    subscription.subscription_created_date,
    subscription.subscription_updated_date,
    subscription.subscription_created_datetime,
    subscription.subscription_updated_datetime,
    subscription.term_start_date,
    subscription.term_end_date,
    subscription.term_start_month,
    subscription.term_end_month,
    subscription.term_start_fiscal_year,
    subscription.term_end_fiscal_year,
    subscription.is_single_fiscal_year_term_subscription,
    subscription.second_active_renewal_month,
    subscription.cancelled_date,

    --Lineage and Cohort Information
    subscription_lineage.subscription_lineage,
    subscription_lineage.oldest_subscription_in_cohort,
    subscription_lineage.subscription_cohort_month,
    subscription_lineage.subscription_cohort_quarter,
    subscription_lineage.subscription_cohort_year

  FROM subscription
  LEFT JOIN subscription_lineage
    ON subscription_lineage.subscription_name_slugify = subscription.subscription_name_slugify
  LEFT JOIN prep_amendment
    ON subscription.dim_amendment_id_subscription = prep_amendment.dim_amendment_id
  LEFT JOIN subscription_opportunity_mapping
    ON subscription.dim_subscription_id = subscription_opportunity_mapping.dim_subscription_id
  LEFT JOIN data_quality_filter_subscription_slugify AS dqf
    ON subscription.subscription_name_slugify = dqf.subscription_name_slugify

), final AS (

    SELECT
      --Surrogate Key
      subscription.dim_subscription_id,

      --Natural Key
      subscription.subscription_name,
      subscription.subscription_version,

      --Common Dimension Keys
      subscription.dim_crm_account_id,
      subscription.dim_billing_account_id,
      subscription.dim_billing_account_id_invoice_owner_account,
      subscription.dim_billing_account_id_creator_account,
      subscription.dim_crm_opportunity_id,
      subscription.dim_crm_opportunity_id_current_open_renewal,
      subscription.dim_crm_opportunity_id_closed_lost_renewal,
      subscription.dim_amendment_id_subscription,

      -- Oldest subscription cohort keys
      oldest_subscription.dim_subscription_id AS dim_oldest_subscription_in_cohort_id,
      oldest_subscription.dim_crm_account_id AS dim_oldest_crm_account_in_cohort_id,

      --Subscription Information
      subscription.created_by_id,
      subscription.updated_by_id,
      subscription.dim_subscription_id_original,
      subscription.dim_subscription_id_previous,
      subscription.subscription_name_slugify,
      subscription.subscription_status,
      subscription.namespace_id,
      subscription.namespace_name,
      subscription.zuora_renewal_subscription_name,
      subscription.zuora_renewal_subscription_name_slugify,
      subscription.current_term,
      subscription.renewal_term,
      subscription.renewal_term_period_type,
      subscription.eoa_starter_bronze_offer_accepted,
      subscription.subscription_sales_type,
      subscription.auto_renew_native_hist,
      subscription.auto_renew_customerdot_hist,
      subscription.turn_on_cloud_licensing,
      subscription.turn_on_operational_metrics,
      subscription.contract_operational_metrics,
      subscription.contract_auto_renewal,
      subscription.turn_on_auto_renewal,
      subscription.contract_seat_reconciliation,
      subscription.turn_on_seat_reconciliation,
      subscription.is_questionable_opportunity_mapping,
      subscription.invoice_owner_account,
      subscription.creator_account,
      subscription.was_purchased_through_reseller,
      subscription.multi_year_deal_subscription_linkage,
      subscription.ramp_id,
      subscription.is_ramp,

      -- Oldest subscription date and cohort information
      oldest_subscription.subscription_start_date   AS oldest_subscription_start_date,
      oldest_subscription.subscription_cohort_month AS oldest_subscription_cohort_month,

      --Date Information
      subscription.subscription_start_date,
      subscription.subscription_end_date,
      subscription.subscription_start_month,
      subscription.subscription_end_month,
      subscription.subscription_end_fiscal_year,
      subscription.subscription_created_date,
      subscription.subscription_updated_date,
      subscription.subscription_updated_datetime,
      subscription.term_start_date,
      subscription.term_end_date,
      subscription.term_start_month,
      subscription.term_end_month,
      subscription.term_start_fiscal_year,
      subscription.term_end_fiscal_year,
      subscription.is_single_fiscal_year_term_subscription,
      subscription.second_active_renewal_month,
      subscription.cancelled_date,

      --Lineage and Cohort Information
      subscription.subscription_lineage,
      subscription.oldest_subscription_in_cohort,
      subscription.subscription_cohort_month,
      subscription.subscription_cohort_quarter,
      subscription.subscription_cohort_year
      
    FROM joined AS subscription
    LEFT JOIN joined AS oldest_subscription
      ON subscription.oldest_subscription_in_cohort = oldest_subscription.subscription_name_slugify
      AND subscription.subscription_status IN ('Active', 'Cancelled')
      AND subscription.is_data_quality_filter_subscription_slugify_flag = 0
      AND oldest_subscription.subscription_status IN ('Active', 'Cancelled')
      AND oldest_subscription.is_data_quality_filter_subscription_slugify_flag = 0
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@mdrussell",
    created_date="2020-12-16",
    updated_date="2024-11-05"
) }}
