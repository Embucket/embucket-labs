{{ config(
    tags=["mnpi_exception"]
) }}

WITH orders AS (

    SELECT 
      -- primary key
      dim_order_id,

      -- foreign keys
      customer_id,
      product_rate_plan_id,
      dim_subscription_id,
      dim_namespace_id,
      dim_billing_account_id,

      -- dates
      order_start_date,
      order_end_date,

      -- order metadata
      order_created_at,
      last_extra_ci_minutes_sync_at,
      increased_billing_rate_notified_at,
      order_is_trial,
      
      -- hist
      amendment_type,
      valid_from,
      valid_to
      FROM {{ref('prep_order_hist')}}

)

{{ dbt_audit(
    cte_ref="orders",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-30",
    updated_date="2021-07-13"
) }}