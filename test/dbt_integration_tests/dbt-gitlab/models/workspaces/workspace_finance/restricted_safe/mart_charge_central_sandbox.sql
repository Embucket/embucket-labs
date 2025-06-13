{{config({
    "materialized": "table",
    "transient": false
  })
}}

{{ simple_cte([
    ('dim_amendment','dim_amendment_central_sandbox'),
    ('dim_billing_account','dim_billing_account_central_sandbox'),
    ('dim_charge','dim_charge_central_sandbox'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_product_detail','dim_product_detail_central_sandbox'),
    ('dim_subscription','dim_subscription_central_sandbox'),
    ('dim_crm_user','dim_crm_user'),
    ('fct_charge','fct_charge_central_sandbox')
]) }}

, mart_charge AS (

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
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_territory                                    AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_region                                       AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                         AS parent_crm_account_area,
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
    ORDER BY dim_crm_account.dim_parent_crm_account_id, dim_crm_account.dim_crm_account_id, fct_charge.subscription_name,
      fct_charge.subscription_version, fct_charge.rate_plan_charge_number, fct_charge.rate_plan_charge_version,
      fct_charge.rate_plan_charge_segment

)

{{ dbt_audit(
    cte_ref="mart_charge",
    created_by="@michellecooper",
    updated_by="@lisvinueza",
    created_date="2022-04-13",
    updated_date="2023-05-22"
) }}
