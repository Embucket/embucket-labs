{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_amendment_central_sandbox AS (

  SELECT *
  FROM {{ ref('prep_amendment_central_sandbox') }}

), subscription_central_sandbox AS (

    SELECT *
    FROM {{ ref('prep_subscription_central_sandbox') }}

), subscription_lineage AS (

    SELECT DISTINCT
      subscription_name_slugify,
      subscription_lineage,
      oldest_subscription_in_cohort,
      subscription_cohort_month,
      subscription_cohort_quarter,
      subscription_cohort_year
    FROM {{ ref('map_subscription_lineage') }}

), final AS (

  SELECT
    --Surrogate Key
    subscription_central_sandbox.dim_subscription_id,

    --Natural Key
    subscription_central_sandbox.subscription_name,
    subscription_central_sandbox.subscription_version,

    --Common Dimension Keys
    subscription_central_sandbox.dim_crm_account_id,
    subscription_central_sandbox.dim_billing_account_id,
    subscription_central_sandbox.dim_billing_account_id_invoice_owner,
    subscription_central_sandbox.dim_crm_opportunity_id,
    {{ get_keyed_nulls('prep_amendment_central_sandbox.dim_amendment_id') }}       AS dim_amendment_id_subscription,

    --Subscription Information
    subscription_central_sandbox.created_by_id,
    subscription_central_sandbox.updated_by_id,
    subscription_central_sandbox.dim_subscription_id_original,
    subscription_central_sandbox.dim_subscription_id_previous,
    subscription_central_sandbox.subscription_name_slugify,
    subscription_central_sandbox.subscription_status,
    subscription_central_sandbox.zuora_renewal_subscription_name,
    subscription_central_sandbox.zuora_renewal_subscription_name_slugify,
    subscription_central_sandbox.current_term,
    subscription_central_sandbox.renewal_term,
    subscription_central_sandbox.renewal_term_period_type,
    subscription_central_sandbox.eoa_starter_bronze_offer_accepted,
    subscription_central_sandbox.subscription_sales_type,
    subscription_central_sandbox.auto_renew_native_hist,
    subscription_central_sandbox.auto_renew_customerdot_hist,
    subscription_central_sandbox.turn_on_cloud_licensing,
    subscription_central_sandbox.turn_on_operational_metrics,
    subscription_central_sandbox.contract_operational_metrics,
    subscription_central_sandbox.contract_auto_renewal,
    subscription_central_sandbox.turn_on_auto_renewal,
    subscription_central_sandbox.contract_seat_reconciliation,
    subscription_central_sandbox.turn_on_seat_reconciliation,

    --Date Information
    subscription_central_sandbox.subscription_start_date,
    subscription_central_sandbox.subscription_end_date,
    subscription_central_sandbox.subscription_start_month,
    subscription_central_sandbox.subscription_end_month,
    subscription_central_sandbox.subscription_end_fiscal_year,
    subscription_central_sandbox.subscription_created_date,
    subscription_central_sandbox.subscription_updated_date,
    subscription_central_sandbox.term_start_date,
    subscription_central_sandbox.term_end_date,
    subscription_central_sandbox.term_start_month,
    subscription_central_sandbox.term_end_month,
    subscription_central_sandbox.second_active_renewal_month,

    --Lineage and Cohort Information
    subscription_lineage.subscription_lineage,
    subscription_lineage.oldest_subscription_in_cohort,
    subscription_lineage.subscription_cohort_month,
    subscription_lineage.subscription_cohort_quarter,
    subscription_lineage.subscription_cohort_year

  FROM subscription_central_sandbox
  LEFT JOIN subscription_lineage
    ON subscription_lineage.subscription_name_slugify = subscription_central_sandbox.subscription_name_slugify
  LEFT JOIN prep_amendment_central_sandbox
    ON subscription_central_sandbox.dim_amendment_id_subscription = prep_amendment_central_sandbox.dim_amendment_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-03-31",
    updated_date="2022-04-13"
) }}