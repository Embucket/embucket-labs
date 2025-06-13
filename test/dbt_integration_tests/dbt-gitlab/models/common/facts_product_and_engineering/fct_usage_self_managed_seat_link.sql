{{ config(
    tags=["mnpi_exception"]
) }}

WITH seat_links AS (

    SELECT *,
      DATE_TRUNC('month', report_date)                              AS snapshot_month
    FROM {{ ref('prep_usage_self_managed_seat_link') }}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        order_subscription_id,
        snapshot_month
      ORDER BY report_date DESC
      ) = 1

), final AS (

    SELECT
      -- ids & keys
      customers_db_order_id                                         AS latest_order_id_in_month,
      dim_subscription_id,
      dim_subscription_id_original,
      dim_subscription_id_previous,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_product_tier_id,
      
      --counts
      seat_links.active_user_count                                  AS active_user_count,
      seat_links.license_user_count,
      max_historical_user_count,
      
      --flags
      is_last_seat_link_report_per_subscription,
      is_last_seat_link_report_per_order,
      is_subscription_in_zuora,
      is_rate_plan_in_zuora,
      is_active_user_count_available,

      --dates
      seat_links.snapshot_month,
      seat_links.report_date     
    FROM seat_links 
      
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-11",
    updated_date="2021-02-08"
) }}