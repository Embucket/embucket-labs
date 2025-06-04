WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

)

, final AS (
    
    SELECT 
      --Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['order_id'])}} AS dim_order_sk,
      
      --Natural Key
      order_id                                   AS internal_order_id, --Can only be joined to CDot Orders
     
     --Foreign Keys
      customer_id                                AS user_id,
      billing_account_id                         AS dim_billing_account_id,
      zuora_account_id,                           
      product_rate_plan_id,                       
      subscription_id                            AS dim_subscription_id,
      subscription_name,
      subscription_name_slugify,
      gitlab_namespace_id                        AS dim_namespace_id,

    --Other attributes
      gitlab_namespace_name                      AS namespace_name,
      order_start_date,
      order_end_date,
      order_quantity,
      order_created_at,
      order_updated_at,
      amendment_type,
      order_is_trial                             AS is_order_trial,
      last_extra_ci_minutes_sync_at,           
      increased_billing_rate_notified_at,
      order_source,
      task_instance

    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-08-09",
    updated_date="2023-08-09"
) }}