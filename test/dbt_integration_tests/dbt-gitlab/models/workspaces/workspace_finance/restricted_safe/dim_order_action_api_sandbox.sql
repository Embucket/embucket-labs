{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ref('zuora_api_sandbox_order_action_source')}}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      
      dim_order_action_id,
      dim_order_id,
      dim_subscription_id,
      dim_amendment_id,
      order_action_type,
      order_action_sequence,
      is_auto_renew,
      cancellation_policy,
      term_type,
      order_action_created_date,
      customer_acceptance_date,
      contract_effective_date,
      service_activation_date,
      current_term,
      current_term_period_type,
      renewal_term,
      renewal_term_period_type,
      renewal_setting,
      term_start_date

    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2022-01-28",
    updated_date="2022-01-28"
) }}