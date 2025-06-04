{{config({
    "materialized": "table",
    "transient": false
  })
}}

{% set renewal_fiscal_years = dbt_utils.get_column_values(
        table=ref('prep_renewal_fiscal_years'),
        where="fiscal_year >= 2019",
        column='fiscal_year',
        order_by='fiscal_year' )%}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge', 'fct_charge'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_amendment', 'dim_amendment')
]) }}

, dim_subscription_source AS (

    SELECT
      dim_subscription.*,
       CASE
         WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
             = term_start_month THEN TRUE
         ELSE FALSE
       END AS is_dup_term
     FROM dim_subscription
     WHERE
       --data quality, last version is expired with no ARR in mart_arr. Should filter it out completely.
       dim_subscription_id NOT IN ('2c92a0ff5e1dcf14015e3bb595f14eef','2c92a0ff5e1dcf14015e3c191d4f7689','2c92a007644967bc01645d54e7df49a8', '2c92a007644967bc01645d54e9b54a4b', '2c92a0ff5e1dcf1a015e3bf7a32475a5')
       --test subscription
       AND subscription_name != 'Test- New Subscription'
       --data quality, last term not entered with same pattern, sub_name = A-S00022101
       AND dim_subscription_id != '2c92a00f7579c362017588a2de19174a'
       --term dates do not align to the subscription term dates, sub_name = A-S00038937
       AND dim_subscription_id != '2c92a01177472c5201774af57f834a43'
       --data quality, last term not entered with same pattern that fits ATR logic. Edge cases that needs to be filtered out to get to the last term version that should count for this subscription.
       --sub_name = A-S00011774
       AND dim_subscription_id NOT IN ('8a1298657dd7f81d017dde1bd9c03fa8','8a128b317dd7e89a017ddd38a74d3037','8a128b317dd7e89a017ddd38a6052ff0',
                                       '8a128b317dc30baa017dc41e5b0932e9','8a128b317dc30baa017dc41e59dd32be','8a128b317dc30baa017dc41e58b43295',
                                       '2c92a0fd7cc1ab13017cc843195f62fb','2c92a0fd7cc1ab13017cc843186f62da','2c92a0fd7cc1ab13017cc843178162b6',
                                       '2c92a0fd7cc1ab13017cc843164d6292','8a128f2a90c6782b0190cb57203d39fa') --e.g. subscription versions where the original term has been shortened due to a cancellation for a contract reset

), dim_subscription_int AS (

    SELECT
      dim_subscription_source.*,
      CASE
        WHEN LEAD(term_end_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_end_month THEN TRUE
        WHEN LEAD(term_end_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_end_month THEN TRUE
        WHEN LEAD(subscription_end_fiscal_year) OVER (PARTITION BY subscription_name ORDER BY
            subscription_version) = subscription_end_fiscal_year THEN TRUE
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            = term_start_month THEN TRUE
        --check for subsequent subscriptiptions that are backed out
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,3) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,4) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,5) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,6) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,7) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,8) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,9) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,10) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,11) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,12) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,13) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,14) OVER (PARTITION BY subscription_name ORDER BY subscription_version)
            < term_start_month THEN TRUE
        ELSE FALSE
      END AS exclude_from_term_sorting
    FROM dim_subscription_source
    WHERE is_dup_term = FALSE

), base_subscriptions AS (

    SELECT
      dim_subscription_id,
      RANK() OVER (PARTITION BY subscription_name, term_start_month ORDER BY subscription_version DESC) AS last_term_version,
      RANK() OVER (PARTITION BY subscription_name ORDER BY subscription_version DESC) AS last_version
    FROM dim_subscription_int
    WHERE exclude_from_term_sorting = FALSE

), dim_subscription_last_term AS (

    SELECT
      dim_subscription.*,
      base_subscriptions.last_version
    FROM dim_subscription
    INNER JOIN base_subscriptions
      ON dim_subscription.dim_subscription_id = base_subscriptions.dim_subscription_id
    WHERE last_term_version = 1

), mart_charge_base AS (

    SELECT
      --Surrogate Key
      dim_charge.dim_charge_id                                                        AS dim_charge_id,

      --Natural Key
      dim_charge.subscription_name                                                    AS subscription_name,
      dim_charge.subscription_version                                                 AS subscription_version,
      dim_charge.rate_plan_charge_number                                              AS rate_plan_charge_number,
      dim_charge.rate_plan_charge_version                                             AS rate_plan_charge_version,
      dim_charge.rate_plan_charge_segment                                             AS rate_plan_charge_segment,

      --Charge Information
      dim_charge.rate_plan_name                                                       AS rate_plan_name,
      dim_charge.rate_plan_charge_name                                                AS rate_plan_charge_name,
      dim_charge.rate_plan_charge_description                                         AS rate_plan_charge_description,
      dim_charge.charge_type                                                          AS charge_type,
      dim_charge.is_paid_in_full                                                      AS is_paid_in_full,
      dim_charge.is_last_segment                                                      AS is_last_segment,
      dim_charge.is_included_in_arr_calc                                              AS is_included_in_arr_calc,
      dim_charge.effective_start_date                                                 AS effective_start_date,
      dim_charge.effective_end_date                                                   AS effective_end_date,
      dim_charge.effective_start_month                                                AS effective_start_month,
      dim_charge.effective_end_month                                                  AS effective_end_month,
      dim_charge.charge_created_date                                                  AS charge_created_date,
      dim_charge.charge_updated_date                                                  AS charge_updated_date,

      --Subscription Information
      dim_subscription.dim_subscription_id                                            AS dim_subscription_id,
      dim_subscription.created_by_id                                                  AS subscription_created_by_id,
      dim_subscription.updated_by_id                                                  AS subscription_updated_by_id,
      dim_subscription.subscription_start_date                                        AS subscription_start_date,
      dim_subscription.subscription_end_date                                          AS subscription_end_date,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,
      dim_subscription.subscription_end_fiscal_year                                   AS subscription_end_fiscal_year,
      dim_subscription.subscription_created_date                                      AS subscription_created_date,
      dim_subscription.subscription_updated_date                                      AS subscription_updated_date,
      dim_subscription.second_active_renewal_month                                    AS second_active_renewal_month,
      dim_subscription.term_start_date,
      dim_subscription.term_end_date,
      dim_subscription.term_start_month,
      dim_subscription.term_end_month,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.auto_renew_native_hist,
      dim_subscription.auto_renew_customerdot_hist,
      dim_subscription.turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics,
      dim_subscription.contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation,
      dim_subscription.is_single_fiscal_year_term_subscription,
      dim_subscription.term_start_fiscal_year,
      dim_subscription.term_end_fiscal_year,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_billing_account.ssp_channel                                                 AS ssp_channel,
      dim_billing_account.po_required                                                 AS po_required,

      -- crm account info
      dim_crm_user.dim_crm_user_id                                                    AS dim_crm_user_id,
      dim_crm_user.crm_user_sales_segment                                             AS crm_user_sales_segment,
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                         AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_upa_country                                  AS parent_crm_account_upa_country,
      dim_crm_account.parent_crm_account_sales_segment                                AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_territory                                    AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_region                                       AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                         AS parent_crm_account_area,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,
      dim_crm_account.is_jihu_account                                                 AS is_jihu_account,

      --Cohort Information
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_quarter,

      --product info
      dim_product_detail.dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,
      dim_product_detail.is_arpu                                                      AS is_arpu,
      dim_product_detail.is_licensed_user                                             AS is_licensed_user,

      --Amendment Information
      dim_subscription.dim_amendment_id_subscription,
      fct_charge.dim_amendment_id_charge,
      dim_amendment_subscription.effective_date                                       AS subscription_amendment_effective_date,
      CASE
        WHEN dim_charge.subscription_version = 1
          THEN 'NewSubscription'
          ELSE dim_amendment_subscription.amendment_type
      END                                                                             AS subscription_amendment_type,
      dim_amendment_subscription.amendment_name                                       AS subscription_amendment_name,
      CASE
        WHEN dim_charge.subscription_version = 1
          THEN 'NewSubscription'
          ELSE dim_amendment_charge.amendment_type
      END                                                                             AS charge_amendment_type,

      --ARR Analysis Framework
      dim_charge.type_of_arr_change,

      --Additive Fields
      fct_charge.mrr,
      fct_charge.previous_mrr,
      fct_charge.delta_mrr,
      fct_charge.arr,
      fct_charge.previous_arr,
      fct_charge.delta_arr,
      fct_charge.quantity,
      fct_charge.previous_quantity,
      fct_charge.delta_quantity,
      fct_charge.delta_tcv,
      fct_charge.estimated_total_future_billings
    FROM fct_charge
    INNER JOIN dim_charge
      ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
    INNER JOIN dim_subscription
      ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    INNER JOIN dim_product_detail
      ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    LEFT JOIN dim_amendment AS dim_amendment_subscription
      ON dim_subscription.dim_amendment_id_subscription = dim_amendment_subscription.dim_amendment_id
    LEFT JOIN dim_amendment AS dim_amendment_charge
      ON fct_charge.dim_amendment_id_charge = dim_amendment_charge.dim_amendment_id
    WHERE dim_crm_account.is_jihu_account != 'TRUE'

), mart_charge AS (

    SELECT mart_charge_base.*
    FROM mart_charge_base
    INNER JOIN dim_subscription_last_term
      ON mart_charge_base.dim_subscription_id = dim_subscription_last_term.dim_subscription_id
    WHERE is_included_in_arr_calc = 'TRUE'
      AND mart_charge_base.term_end_month = mart_charge_base.effective_end_month
      AND arr != 0

{% for renewal_fiscal_year in renewal_fiscal_years -%}
), renewal_subscriptions_{{renewal_fiscal_year}} AS (

    SELECT DISTINCT
      sub_1.subscription_name,
      sub_1.zuora_renewal_subscription_name,
      DATE_TRUNC('month',sub_2.subscription_end_date) AS subscription_end_month
    FROM dim_subscription_last_term sub_1
    INNER JOIN dim_subscription_last_term sub_2
      ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
    WHERE sub_1.zuora_renewal_subscription_name != ''
      AND sub_1.last_version = 1
      AND sub_2.last_version = 1

), base_{{renewal_fiscal_year}} AS (--get the base data set of recurring charges.

    SELECT
      mart_charge.dim_charge_id,
      mart_charge.dim_crm_account_id,
      mart_charge.dim_billing_account_id,
      mart_charge.dim_subscription_id,
      mart_charge.dim_product_detail_id,
      mart_charge.parent_crm_account_name,
      mart_charge.crm_account_name,
      mart_charge.parent_crm_account_sales_segment,
      dim_crm_user.dim_crm_user_id,
      dim_crm_user.user_name,
      dim_crm_user.user_role_id,
      dim_crm_user.crm_user_sales_segment,
      dim_crm_user.crm_user_geo,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      mart_charge.product_tier_name,
      mart_charge.is_arpu,
      mart_charge.is_licensed_user,
      mart_charge.product_delivery_type,
      mart_charge.subscription_name,
      dim_subscription_last_term.zuora_renewal_subscription_name,
      dim_subscription_last_term.current_term,
      mart_charge.is_single_fiscal_year_term_subscription,
      CASE
        WHEN dim_subscription_last_term.current_term >= 24
          THEN TRUE
        WHEN dim_subscription_last_term.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions_{{renewal_fiscal_year}})
          THEN TRUE
        ELSE FALSE
      END                                                                                                                                               AS is_multi_year_booking,
      CASE
        WHEN dim_subscription_last_term.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions_{{renewal_fiscal_year}})
          THEN TRUE
        ELSE FALSE
      END                                                                                                                                               AS is_multi_year_booking_with_multi_subs,
      mart_charge.is_paid_in_full,
      mart_charge.estimated_total_future_billings,
      mart_charge.effective_start_month,
      mart_charge.effective_end_month,
      mart_charge.subscription_start_month,
      mart_charge.subscription_end_month,
      mart_charge.term_start_month,
      mart_charge.term_end_month,
      DATEADD('month',-1,mart_charge.term_end_month)                                                                                                    AS last_paid_month_in_term,
      renewal_subscriptions_{{renewal_fiscal_year}}.subscription_end_month                                                                              AS multi_year_booking_subscription_end_month,
      DATEDIFF(month,mart_charge.effective_start_month,mart_charge.effective_end_month)                                                                 AS charge_term,
      mart_charge.quantity,
      mart_charge.arr
    FROM mart_charge
    LEFT JOIN dim_subscription_last_term
      ON mart_charge.dim_subscription_id = dim_subscription_last_term.dim_subscription_id
    LEFT JOIN dim_crm_account
      ON mart_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    LEFT JOIN renewal_subscriptions_{{renewal_fiscal_year}}
      ON mart_charge.subscription_name = renewal_subscriptions_{{renewal_fiscal_year}}.subscription_name
    WHERE ( mart_charge.term_start_month <= CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')
      AND mart_charge.term_end_month > CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')
      )
      OR (
          mart_charge.is_single_fiscal_year_term_subscription = TRUE
          AND mart_charge.term_start_fiscal_year = '{{renewal_fiscal_year}}'
        )

), agg_charge_term_less_than_equal_12_{{renewal_fiscal_year}} AS (--get the starting and ending month ARR for charges with current terms <= 12 months. These terms do not need additional logic.

    SELECT
      CASE
        WHEN is_multi_year_booking = TRUE THEN 'MYB'
        ELSE 'Non-MYB'
      END                             AS renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      --charge_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      term_end_month,
      subscription_end_month,
      SUM(arr)      AS arr
    FROM base_{{renewal_fiscal_year}}
    WHERE current_term <= 12
    {{ dbt_utils.group_by(n=23) }}

), agg_charge_term_greater_than_12_{{renewal_fiscal_year}} AS (--get the starting and ending month ARR for terms > 12 months. These terms need additional logic.

    SELECT
      CASE
        WHEN is_multi_year_booking = TRUE THEN 'MYB'
        ELSE 'Non-MYB'
      END                                   AS renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      --current_term,
      CASE--the below odd term charges do not behave well in the multi-year bookings logic and end up with duplicate renewals in the fiscal year. This CASE statement smooths out the charges so they only have one renewal entry in the fiscal year.
        WHEN current_term = 25 THEN 24
        WHEN current_term = 26 THEN 24
        WHEN current_term = 27 THEN 36
        WHEN current_term = 28 THEN 36
        WHEN current_term = 29 THEN 36
        WHEN current_term = 30 THEN 36
        WHEN current_term = 31 THEN 36
        WHEN current_term = 32 THEN 36
        WHEN current_term = 35 THEN 36
        WHEN current_term = 37 THEN 36
        WHEN current_term = 38 THEN 36
        WHEN current_term = 41 THEN 36
        WHEN current_term = 42 THEN 48
        WHEN current_term = 49 THEN 48
        WHEN current_term = 57 THEN 60
        ELSE current_term
      END                                   AS current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      term_end_month,
      subscription_end_month,
      SUM(arr)                              AS arr
    FROM base_{{renewal_fiscal_year}}
    WHERE current_term > 12
    {{ dbt_utils.group_by(n=23) }}

), twenty_four_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for multi-year charges that are not in the Zuora data. The start and end months are in the agg_myb for multi-year bookings.

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/2,term_start_month)  AS term_end_month,
      subscription_end_month,
      SUM(arr)                                          AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 13 AND 24
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

), thirty_six_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for multi-year bookings that are not in the Zuora data. The start and end months are in the agg_myb for multi-year bookings.

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/3,term_start_month)      AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 25 AND 36
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/3*2,term_start_month)    AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 25 AND 36
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}
    ORDER BY 1

), forty_eight_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for multi-year bookings that are not in the Zuora data. The start and end months are in the agg_MYB for multi-year bookings.

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/4,term_start_month)      AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 37 AND 48
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/4*2,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 37 AND 48
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/4*3,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 37 AND 48
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}
    ORDER BY 1

), sixty_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for multi-year bookings that are not in the Zuora data. The start and end months are in the agg_MYB for multi-year bookings.

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/5,term_start_month)          AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 49 AND 60
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/5*2,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 49 AND 60
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/5*3,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 49 AND 60
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}

    UNION ALL

    SELECT
      renewal_type,
      is_multi_year_booking,
      is_multi_year_booking_with_multi_subs,
      is_single_fiscal_year_term_subscription,
      current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      DATEADD('month',current_term/5*4,term_start_month)    AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    WHERE current_term BETWEEN 49 AND 60 AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')
    {{ dbt_utils.group_by(n=23) }}
    ORDER BY 1

), combined_{{renewal_fiscal_year}} AS (--union all of the charges

    SELECT *
    FROM agg_charge_term_less_than_equal_12_{{renewal_fiscal_year}}

    UNION ALL

    SELECT *
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}

    UNION ALL

    SELECT *
    FROM twenty_four_mth_term_{{renewal_fiscal_year}}

    UNION ALL

    SELECT *
    FROM thirty_six_mth_term_{{renewal_fiscal_year}}

    UNION ALL

    SELECT *
    FROM forty_eight_mth_term_{{renewal_fiscal_year}}

    UNION ALL

    SELECT *
    FROM sixty_mth_term_{{renewal_fiscal_year}}

), opportunity_term_group AS (

    SELECT
      dim_subscription.dim_subscription_id,
      dim_crm_opportunity.dim_crm_opportunity_id,
      CASE
        WHEN close_date IS NULL THEN '1951-01-01'
        ELSE DATE_TRUNC('month',close_date)
      END                                     AS close_month,
      CASE
        WHEN dim_crm_opportunity.opportunity_term = 0
          THEN '0 Years'
        WHEN dim_crm_opportunity.opportunity_term <= 12
          THEN '1 Year'
        WHEN dim_crm_opportunity.opportunity_term > 12
          AND dim_crm_opportunity.opportunity_term <= 24
            THEN '2 Years'
        WHEN dim_crm_opportunity.opportunity_term > 24
          AND dim_crm_opportunity.opportunity_term <= 36
            THEN '3 Years'
        WHEN dim_crm_opportunity.opportunity_term > 36
          THEN '4 Years+'
        WHEN dim_crm_opportunity.opportunity_term IS NULL
          THEN 'No Opportunity Term'
      END                                                                                                               AS opportunity_term_group
    FROM dim_subscription
    LEFT JOIN dim_crm_opportunity
      ON dim_subscription.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN fct_crm_opportunity
      ON dim_subscription.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id

), renewal_report_{{renewal_fiscal_year}} AS (--create the renewal report for the applicable fiscal year.

    SELECT
      CONCAT(dim_date.fiscal_quarter_name_fy, base_{{renewal_fiscal_year}}.term_end_month, base_{{renewal_fiscal_year}}.dim_charge_id)      AS concat_primary_key,
      {{ dbt_utils.generate_surrogate_key(['concat_primary_key' ]) }}                                                                                AS primary_key,
      dim_date.fiscal_year                                                                                                                  AS fiscal_year,
      dim_date.fiscal_quarter_name_fy                                                                                                       AS fiscal_quarter_name_fy,
      opportunity_term_group.close_month                                                                                                    AS close_month,
      base_{{renewal_fiscal_year}}.dim_charge_id                                                                                            AS dim_charge_id,
      opportunity_term_group.dim_crm_opportunity_id                                                                                         AS dim_crm_opportunity_id,
      base_{{renewal_fiscal_year}}.dim_crm_account_id                                                                                       AS dim_crm_account_id,
      base_{{renewal_fiscal_year}}.dim_billing_account_id                                                                                   AS dim_billing_account_id,
      base_{{renewal_fiscal_year}}.dim_subscription_id                                                                                      AS dim_subscription_id,
      base_{{renewal_fiscal_year}}.dim_product_detail_id                                                                                    AS dim_product_detail_id,
      base_{{renewal_fiscal_year}}.subscription_name                                                                                        AS subscription_name,
      base_{{renewal_fiscal_year}}.subscription_start_month                                                                                 AS subscription_start_month,
      base_{{renewal_fiscal_year}}.subscription_end_month                                                                                   AS subscription_end_month,
      base_{{renewal_fiscal_year}}.term_start_month                                                                                         AS term_start_month,
      base_{{renewal_fiscal_year}}.term_end_month                                                                                           AS renewal_month,
      combined_{{renewal_fiscal_year}}.term_end_month                                                                                       AS bookings_term_end_month,
      base_{{renewal_fiscal_year}}.multi_year_booking_subscription_end_month                                                                AS multi_year_booking_subscription_end_month,
      base_{{renewal_fiscal_year}}.last_paid_month_in_term                                                                                  AS last_paid_month_in_term,
      base_{{renewal_fiscal_year}}.current_term                                                                                             AS current_term,
      renewal_subscriptions_{{renewal_fiscal_year}}.zuora_renewal_subscription_name                                                         AS zuora_renewal_subscription_name,
      renewal_subscriptions_{{renewal_fiscal_year}}.subscription_end_month                                                                  AS renewal_subscription_end_month,
      base_{{renewal_fiscal_year}}.parent_crm_account_name                                                                                  AS parent_crm_account_name,
      base_{{renewal_fiscal_year}}.crm_account_name                                                                                         AS crm_account_name,
      base_{{renewal_fiscal_year}}.parent_crm_account_sales_segment                                                                         AS parent_crm_account_sales_segment,
      base_{{renewal_fiscal_year}}.dim_crm_user_id                                                                                          AS dim_crm_user_id,
      base_{{renewal_fiscal_year}}.user_name                                                                                                AS user_name,
      base_{{renewal_fiscal_year}}.user_role_id                                                                                             AS user_role_id,
      base_{{renewal_fiscal_year}}.crm_user_sales_segment                                                                                   AS crm_user_sales_segment,
      base_{{renewal_fiscal_year}}.crm_user_geo                                                                                             AS crm_user_geo,
      base_{{renewal_fiscal_year}}.crm_user_region                                                                                          AS crm_user_region,
      base_{{renewal_fiscal_year}}.crm_user_area                                                                                            AS crm_user_area,
      base_{{renewal_fiscal_year}}.product_tier_name                                                                                        AS product_tier_name,
      base_{{renewal_fiscal_year}}.product_delivery_type                                                                                    AS product_delivery_type,
      combined_{{renewal_fiscal_year}}.renewal_type                                                                                         AS renewal_type,
      base_{{renewal_fiscal_year}}.is_arpu                                                                                                  AS is_arpu,
      base_{{renewal_fiscal_year}}.is_licensed_user                                                                                         AS is_licensed_user,
      base_{{renewal_fiscal_year}}.is_multi_year_booking                                                                                    AS is_multi_year_booking,
      base_{{renewal_fiscal_year}}.is_multi_year_booking_with_multi_subs                                                                    AS is_multi_year_booking_with_multi_subs,
      base_{{renewal_fiscal_year}}.is_single_fiscal_year_term_subscription                                                                  AS is_single_fiscal_year_term_subscription,
      base_{{renewal_fiscal_year}}.current_term                                                                                             AS subscription_term,
      base_{{renewal_fiscal_year}}.estimated_total_future_billings                                                                          AS estimated_total_future_billings,
      CASE
        WHEN base_{{renewal_fiscal_year}}.term_end_month BETWEEN DATEADD('month',1, CONCAT('{{renewal_fiscal_year}}'-1,'-01-01'))
          AND CONCAT('{{renewal_fiscal_year}}','-01-01')
            AND base_{{renewal_fiscal_year}}.is_multi_year_booking_with_multi_subs = FALSE
            THEN TRUE
        ELSE FALSE
      END                                                                                                                                   AS is_available_to_renew,
      CASE
        WHEN opportunity_term_group.opportunity_term_group IS NULL
          THEN 'No Opportunity Term'
        ELSE opportunity_term_group.opportunity_term_group
      END                                                                                                                                   AS opportunity_term_group,
      base_{{renewal_fiscal_year}}.quantity                                                                                                 AS quantity,
      base_{{renewal_fiscal_year}}.arr                                                                                                      AS arr
    FROM combined_{{renewal_fiscal_year}}
    LEFT JOIN dim_date
      ON combined_{{renewal_fiscal_year}}.term_end_month = dim_date.first_day_of_month
    LEFT JOIN base_{{renewal_fiscal_year}}
      ON combined_{{renewal_fiscal_year}}.dim_charge_id = base_{{renewal_fiscal_year}}.dim_charge_id
    LEFT JOIN renewal_subscriptions_{{renewal_fiscal_year}}
      ON base_{{renewal_fiscal_year}}.subscription_name = renewal_subscriptions_{{renewal_fiscal_year}}.subscription_name
    LEFT JOIN opportunity_term_group
      ON base_{{renewal_fiscal_year}}.dim_subscription_id = opportunity_term_group.dim_subscription_id
    WHERE combined_{{renewal_fiscal_year}}.term_end_month BETWEEN DATEADD('month',1, CONCAT('{{renewal_fiscal_year}}'-1,'-01-01'))
      AND CONCAT('{{renewal_fiscal_year}}','-01-01')
        AND day_of_month = 1
    ORDER BY fiscal_quarter_name_fy

{% endfor -%}
), unioned as (

{% for renewal_fiscal_year in renewal_fiscal_years-%}

    SELECT
    primary_key,
    fiscal_year,
    fiscal_quarter_name_fy,
    close_month,
    dim_charge_id,
    dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_subscription_id,
    dim_product_detail_id,
    subscription_name,
    subscription_start_month,
    subscription_end_month,
    term_start_month,
    renewal_month,
    bookings_term_end_month,
    multi_year_booking_subscription_end_month,
    last_paid_month_in_term,
    current_term,
    zuora_renewal_subscription_name,
    renewal_subscription_end_month,
    parent_crm_account_name,
    crm_account_name,
    parent_crm_account_sales_segment,
    dim_crm_user_id,
    user_name,
    user_role_id,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    product_tier_name,
    product_delivery_type,
    renewal_type,
    is_multi_year_booking,
    is_multi_year_booking_with_multi_subs,
    is_single_fiscal_year_term_subscription,
    is_arpu,
    is_licensed_user,
    subscription_term,
    estimated_total_future_billings,
    is_available_to_renew,
    opportunity_term_group,
    quantity,
    arr
    FROM renewal_report_{{renewal_fiscal_year}}
    {%- if not loop.last %} UNION ALL {%- endif %}

{% endfor -%}

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@michellecooper",
    updated_by="@apiaseczna",
    created_date="2021-12-06",
    updated_date="2024-08-01"
) }}
