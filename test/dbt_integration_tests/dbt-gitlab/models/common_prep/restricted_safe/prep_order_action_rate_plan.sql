{{ simple_cte([
    ('zuora_order', 'zuora_order_source'),
    ('zuora_order_action_rate_plan', 'zuora_query_api_order_action_rate_plan_source'),
    ('zuora_order_action', 'zuora_order_action_source'),
    ('zuora_rate_plan', 'zuora_rate_plan_source')
]) }}

, final AS (

    SELECT
      zuora_order_action.order_action_id                                AS dim_order_action_id,
      zuora_order.order_id                                              AS dim_order_id,
      zuora_rate_plan.subscription_id                                   AS dim_subscription_id,
      zuora_order_action.amendment_id                                   AS dim_amendment_id,
      zuora_order.order_number, 
      zuora_rate_plan.rate_plan_id,
      zuora_rate_plan.rate_plan_name, 
      zuora_rate_plan.product_rate_plan_id,
      zuora_rate_plan.amendement_type,
      zuora_order_action.type                                           AS order_action_type,
      zuora_order_action.sequence                                       AS order_action_sequence,
      zuora_order_action.created_date                                   AS order_action_created_date,
      zuora_rate_plan.created_date                                      AS rate_plan_created_date
    FROM zuora_order
    INNER JOIN zuora_order_action 
      ON zuora_order_action.order_id = zuora_order.order_id
    INNER JOIN zuora_order_action_rate_plan 
      ON zuora_order_action_rate_plan.order_action_id = zuora_order_action.order_action_id
    INNER JOIN zuora_rate_plan 
      ON zuora_rate_plan.rate_plan_id = zuora_order_action_rate_plan.rate_plan_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2023-01-31",
    updated_date="2023-04-11"
) }}
