WITH base AS (

    SELECT *
    FROM {{ref('zuora_order_action_source')}}

), final AS (

    SELECT
      
      order_action_id                   AS dim_order_action_id,
      order_id                          AS dim_order_id,
      subscription_id                   AS dim_subscription_id,
      amendment_id                      AS dim_amendment_id,
      type                              AS order_action_type,
      sequence                          AS order_action_sequence,
      auto_renew                        AS is_auto_renew,
      cancellation_policy,
      term_type,
      created_date                      AS order_action_created_date,
      customer_acceptance_date,
      contract_effective_date,
      service_activation_date,
      current_term,
      current_term_period_type,
      renewal_term,
      renewal_term_period_type,
      renew_setting                     AS renewal_setting,
      term_start_date

    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-06-06",
    updated_date="2022-06-06"
) }}