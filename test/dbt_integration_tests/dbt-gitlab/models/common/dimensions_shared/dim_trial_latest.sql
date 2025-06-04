{{ config(
    tags=["product", "mnpi_exception"]
) }}


WITH trial_snapshot AS (

  SELECT *
  FROM  {{ ref('customers_db_orders_snapshots_base')}}
  WHERE order_is_trial = TRUE

)

, latest_trial_per_namespace AS (

    SELECT

      --Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['trial_snapshot.order_snapshot_id'])}}                  AS dim_trial_latest_sk,

      --Natural Key
      trial_snapshot.order_snapshot_id                                                    AS dim_trial_snapshot_id,

      --Foreign Keys
      trial_snapshot.order_id                                                             AS order_id,
      trial_snapshot.gitlab_namespace_id                                                  AS dim_namespace_id,
      trial_snapshot.customer_id                                                          AS customer_id,
      trial_snapshot.product_rate_plan_id                                                 AS dim_product_rate_plan_id,
      trial_snapshot.subscription_id                                                      AS dim_subscription_id,

      --Other Attributes
      trial_snapshot.subscription_name,
      trial_snapshot.subscription_name_slugify,
      trial_snapshot.gitlab_namespace_name,
      trial_snapshot.order_start_date,
      trial_snapshot.order_end_date,
      trial_snapshot.order_quantity,
      trial_snapshot.order_created_at,
      trial_snapshot.order_updated_at,
      trial_snapshot.amendment_type,
      trial_snapshot.order_is_trial                                                      AS is_order_trial,
      trial_snapshot.last_extra_ci_minutes_sync_at,
      trial_snapshot.valid_from,
      trial_snapshot.valid_to

    FROM trial_snapshot
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(gitlab_namespace_id) ORDER BY valid_from DESC, order_id DESC) = 1

)

{{ dbt_audit(
    cte_ref="latest_trial_per_namespace",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-06-26",
    updated_date="2023-06-26"
) }}
