{% macro mart_arr_with_zero_dollar_charges_source(fct_table) %}

{{ config(
    materialized="incremental",
    unique_key="primary_key"
) }}

{{ simple_cte([
    ('fct_mrr', fct_table),
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription')
]) }}

, fct_mrr_all AS (

    SELECT
      dim_date_id,
      date_actual,
      fiscal_quarter_name_fy,
      fiscal_year,
      dim_subscription_id,
      dim_product_detail_id,
      dim_billing_account_id,
      dim_crm_account_id,
      dim_charge_id,
      SUM(mrr)                                                                      AS mrr,
      SUM(arr)                                                                      AS arr,
      SUM(quantity)                                                                 AS quantity,
      ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure)   AS unit_of_measure

    FROM fct_mrr
    WHERE subscription_status IN ('Active', 'Cancelled')
    {{ dbt_utils.group_by(n=9) }}

), joined AS (

    SELECT
      --primary_key
      fct_mrr_all.dim_date_id                                                         AS dim_date_id,
      fct_mrr_all.dim_charge_id                                                       AS dim_charge_id,
      --date info
      fct_mrr_all.date_actual                                                         AS date_actual,
      fct_mrr_all.fiscal_quarter_name_fy                                              AS fiscal_quarter_name_fy,
      fct_mrr_all.fiscal_year                                                         AS fiscal_year,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_billing_account.ssp_channel                                                 AS ssp_channel,
      dim_billing_account.po_required                                                 AS po_required,
      dim_billing_account.auto_pay                                                    AS auto_pay,
      dim_billing_account.default_payment_method_type                                 AS default_payment_method_type,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                         AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment                                AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_sales_segment_legacy                         AS parent_crm_account_sales_segment_legacy,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.crm_account_employee_count_band                                 AS crm_account_employee_count_band,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,
      dim_crm_account.is_jihu_account                                                 AS is_jihu_account,
      dim_crm_account.parent_crm_account_lam                                          AS parent_crm_account_lam,
      dim_crm_account.parent_crm_account_lam_dev_count                                AS parent_crm_account_lam_dev_count,
      dim_crm_account.parent_crm_account_business_unit                                AS parent_crm_account_business_unit,
      dim_crm_account.parent_crm_account_geo                                          AS parent_crm_account_geo,
      dim_crm_account.parent_crm_account_region                                       AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                         AS parent_crm_account_area,
      dim_crm_account.parent_crm_account_role_type                                    AS parent_crm_account_role_type,
      dim_crm_account.parent_crm_account_territory                                    AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_max_family_employee                          AS parent_crm_account_max_family_employee,
      dim_crm_account.parent_crm_account_upa_country                                  AS parent_crm_account_upa_country,
      dim_crm_account.parent_crm_account_upa_state                                    AS parent_crm_account_upa_state,
      dim_crm_account.parent_crm_account_upa_city                                     AS parent_crm_account_upa_city,
      dim_crm_account.parent_crm_account_upa_street                                   AS parent_crm_account_upa_street,
      dim_crm_account.parent_crm_account_upa_postal_code                              AS parent_crm_account_upa_postal_code,
      dim_crm_account.crm_account_employee_count                                      AS crm_account_employee_count,

      --subscription info
      dim_subscription.dim_subscription_id                                            AS dim_subscription_id,
      dim_subscription.dim_subscription_id_original                                   AS dim_subscription_id_original,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name                                              AS subscription_name,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      dim_billing_account.billing_account_arr_cohort_month                            AS billing_account_cohort_month,
      dim_billing_account.billing_account_arr_cohort_quarter                          AS billing_account_cohort_quarter,
      dim_crm_account.crm_account_arr_cohort_month                                    AS crm_account_cohort_month,
      dim_crm_account.crm_account_arr_cohort_quarter                                  AS crm_account_cohort_quarter,
      dim_crm_account.parent_account_arr_cohort_month                                 AS parent_account_cohort_month,
      dim_crm_account.parent_account_arr_cohort_quarter                               AS parent_account_cohort_quarter,
      dim_subscription.auto_renew_native_hist                                         AS auto_renew_native_hist,
      dim_subscription.auto_renew_customerdot_hist                                    AS auto_renew_customerdot_hist,
      dim_subscription.turn_on_cloud_licensing                                        AS turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics                                    AS turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics                                   AS contract_operational_metrics,
      dim_subscription.contract_auto_renewal                                          AS contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal                                           AS turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation                                   AS contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation                                    AS turn_on_seat_reconciliation,
      dim_subscription.invoice_owner_account                                          AS invoice_owner_account,
      dim_subscription.creator_account                                                AS creator_account,
      dim_subscription.was_purchased_through_reseller                                 AS was_purchased_through_reseller,

      --product info
      dim_product_detail.dim_product_detail_id                                        AS dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.product_deployment_type                                      AS product_deployment_type,
      dim_product_detail.product_category                                             AS product_category,
      dim_product_detail.product_rate_plan_category                                   AS product_rate_plan_category,
      dim_product_detail.product_ranking                                              AS product_ranking,
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_charge_name                                AS product_rate_plan_charge_name,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,
      dim_product_detail.is_licensed_user                                             AS is_licensed_user,
      dim_product_detail.is_licensed_user_base_product                                AS is_licensed_user_base_product,
      dim_product_detail.is_licensed_user_add_on                                      AS is_licensed_user_add_on,
      dim_product_detail.is_arpu                                                      AS is_arpu,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr_all.unit_of_measure                                                     AS unit_of_measure,
      fct_mrr_all.mrr                                                                 AS mrr,
      fct_mrr_all.arr                                                                 AS arr,
      fct_mrr_all.quantity                                                            AS quantity
    FROM fct_mrr_all
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr_all.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr_all.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr_all.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE dim_crm_account.is_jihu_account != 'TRUE'

)

{% endmacro %}
