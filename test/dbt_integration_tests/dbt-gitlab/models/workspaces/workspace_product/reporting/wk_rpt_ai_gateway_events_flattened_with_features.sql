{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('wk_mart_behavior_structured_event_ai_gateway_flattened','wk_mart_behavior_structured_event_ai_gateway_flattened')
    ])
}},

base AS (

    SELECT 
        e.*
        EXCLUDE(created_by, updated_by, model_created_date, model_updated_date, dbt_created_at, dbt_updated_at),
        behavior_at::DATE AS behavior_date,
        CASE
            WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN enabled_by_ultimate_parent_namespace_id::VARCHAR
            WHEN dim_installation_id IS NOT NULL THEN dim_installation_id
        END AS enabled_by_product_entity_id,
        CASE
            WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN 'ultimate_parent_namespace_id'
            WHEN dim_installation_id IS NOT NULL THEN 'dim_installation_id'
        END AS enabled_by_product_entity_type,
        CASE
            WHEN enabled_by_product_deployment_type = 'GitLab.com' THEN enabled_by_internal_namespace
            WHEN dim_installation_id IS NOT NULL THEN enabled_by_internal_installation
            ELSE FALSE
        END AS enabled_by_internal_product_entity,
        CASE 
            WHEN enabled_by_product_tier_names_at_event_time IS NULL THEN 'No Product Tier Subscription' 
            ELSE ARRAY_TO_STRING(enabled_by_product_tier_names_at_event_time, ', ')
        END AS enabled_by_product_tier,
        CASE
            WHEN enabled_by_add_on_product_at_event_time LIKE '%Duo Pro%' THEN 'Duo Pro Subscription'
            WHEN enabled_by_add_on_product_at_event_time LIKE '%Duo Enterprise%' THEN 'Duo Enterprise Subscription'
            ELSE NULL
        END AS duo_subscriptions_clean, -- to be used in enabled_by_duo_add_on
        CASE 
            WHEN duo_subscriptions_clean IS NOT NULL AND enabled_by_add_on_trial_product_at_event_time IS NOT NULL THEN duo_subscriptions_clean || ', ' || enabled_by_add_on_trial_product_at_event_time
            WHEN duo_subscriptions_clean IS NOT NULL THEN duo_subscriptions_clean
            WHEN enabled_by_add_on_trial_product_at_event_time IS NOT NULL THEN enabled_by_add_on_trial_product_at_event_time
            ELSE 'None'
        END AS enabled_by_duo_add_on_detail,
            CASE 
            WHEN duo_subscriptions_clean IS NOT NULL THEN 'Duo Subscription'
            WHEN enabled_by_add_on_trial_product_at_event_time IS NOT NULL THEN 'Duo Trial'
            ELSE 'None'
        END AS enabled_by_duo_add_on, -- simplified into sub, trial, and none categories
        -- returning usage prep calculations below
        DENSE_RANK() OVER(PARTITION BY gitlab_global_user_id 
                      ORDER BY behavior_date) 
            AS any_ai_gw_usage_day_n,
        LAG(behavior_date) OVER(PARTITION BY gitlab_global_user_id 
                           ORDER BY behavior_date) 
            AS any_ai_gw_last_usage_date, 
        DATEDIFF('day', any_ai_gw_last_usage_date, behavior_date) 
            AS days_since_last_any_ai_gw_usage,
        DENSE_RANK() OVER(PARTITION BY gitlab_global_user_id, unit_primitive 
                      ORDER BY behavior_date) 
            AS up_usage_day_n,
        LAG(behavior_date) OVER(PARTITION BY gitlab_global_user_id, unit_primitive 
                           ORDER BY behavior_date) 
            AS up_last_usage_date, 
        DATEDIFF('day', up_last_usage_date, behavior_date) 
            AS days_since_last_up_usage
    FROM wk_mart_behavior_structured_event_ai_gateway_flattened e

), final AS (

    SELECT 
        *
        EXCLUDE(any_ai_gw_last_usage_date, up_last_usage_date, any_ai_gw_usage_day_n, days_since_last_any_ai_gw_usage, up_usage_day_n, days_since_last_up_usage),
        CASE  
          WHEN any_ai_gw_usage_day_n = 1 THEN 'New (Any AI Feature)'
          WHEN days_since_last_any_ai_gw_usage <= 7 THEN 'Returning - Last 7 Days (Any AI Feature)'
          ELSE NULL -- greater than 7 days since latest usage
        END AS any_ai_gw_7day_return_user_status,
        CASE 
          WHEN any_ai_gw_usage_day_n = 1 THEN 'New (Any AI Feature)'
          WHEN days_since_last_any_ai_gw_usage <= 30 THEN 'Returning - Last 30 Days (Any AI Feature)'
          ELSE NULL -- greater than 30 days since latest usage
        END AS any_ai_gw_30day_return_user_status,
        CASE 
          WHEN any_ai_gw_usage_day_n = 1 THEN 'New (Any AI Feature)'
          ELSE 'Returning - All-Time (Any AI Feature)'
        END AS any_ai_gw_alltime_return_user_status,
        CASE 
          WHEN any_ai_gw_usage_day_n = 1 THEN 'New User (Any AI Feature)'
          WHEN days_since_last_any_ai_gw_usage <= 7 THEN 'Returned Within 7 Days (Any AI Feature)'
          WHEN days_since_last_any_ai_gw_usage <= 30 THEN 'Returned Within 30 Days (Any AI Feature)'
          ELSE 'Returned After 30 Days (Any AI Feature)'
        END AS any_ai_gw_return_user_category,
        CASE 
          WHEN up_usage_day_n = 1 THEN 'New (Unit Primitive Specific)'
          WHEN days_since_last_up_usage <= 7 THEN 'Returning - Last 7 Days (Unit Primitive Specific)'
          ELSE NULL -- greater than 7 days since latest usage
        END AS up_7day_return_user_status,
        CASE 
          WHEN up_usage_day_n = 1 THEN 'New (Unit Primitive Specific)'
          WHEN days_since_last_up_usage <= 30 THEN 'Returning - Last 30 Days (Unit Primitive Specific)'
          ELSE NULL -- greater than 30 days since latest usage
        END AS up_30day_return_user_status,
        CASE 
          WHEN up_usage_day_n = 1 THEN 'New (Unit Primitive Specific)'
          ELSE 'Returning - All-Time (Unit Primitive Specific)'
        END AS up_alltime_return_user_status,
        CASE 
          WHEN up_usage_day_n = 1 THEN 'New User (Unit Primitive Specific)'
          WHEN days_since_last_up_usage <= 7 THEN 'Returned Within 7 Days (Unit Primitive Specific)'
          WHEN days_since_last_up_usage <= 30 THEN 'Returned Within 30 Days (Unit Primitive Specific)'
          ELSE 'Returned After 30 Days (Unit Primitive Specific)'
        END AS up_return_user_category,
        feature_category AS feature
    FROM base



)

	{{ dbt_audit(
    cte_ref="final",
    created_by="@eneuberger",
    updated_by="@michellecooper",
    created_date="2024-11-18",
    updated_date="2025-02-24"
) }}