{{ config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk'
) }}

{{ simple_cte([
    ('mart_behavior_structured_event','mart_behavior_structured_event')
]) }}

, final AS (

  SELECT
    {{ dbt_utils.star(from=ref('mart_behavior_structured_event'), except=["CREATED_BY", "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM mart_behavior_structured_event
  WHERE behavior_at > '2022-03-07' --filtering down to when the correct tracking was first added for the purchase funnel

  {% if is_incremental() %}

      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})

  {% endif %}

    AND app_id IN (
      'gitlab',
      'gitlab_customers'
    )
    AND event_category IN (
      'subscriptions:new',
      'SubscriptionsController'
    )
    AND event_action IN (
      'render',
      'click_button'
    )
    AND event_label IN (
      'saas_checkout',
      'continue_billing',
      'continue_payment',
      'review_order',
      'confirm_purchase',
      'update_plan_type',
      'update_group',
      'update_seat_count',
      'select_country',
      'state',
      'saas_checkout_postal_code',
      'tax_link',
      'edit'
    )
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2023-07-18",
    updated_date="2023-07-21"
) }}
