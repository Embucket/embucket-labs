{{ config(
    tags=["product", "mnpi_exception"],
snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ simple_cte([
    ('namespaces', 'prep_namespace'),
    ('subscriptions', 'prep_subscription'),
    ('orders', 'prep_order'),
    ('product_tiers', 'prep_product_tier'),
    ('product_details', 'dim_product_detail'),
    ('fct_mrr_with_zero_dollar_charges', 'fct_mrr_with_zero_dollar_charges'),
    ('trials', 'prep_namespace_order_trial'),
    ('subscription_delivery_types', 'bdg_subscription_product_rate_plan')
]) }}

, product_rate_plans AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name
    FROM product_details
    WHERE product_deployment_type = 'GitLab.com'

), saas_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      product_rate_plan_id,
      dim_product_tier_id
    FROM subscription_delivery_types
    WHERE product_deployment_type = 'GitLab.com'

), trial_tiers AS (

    SELECT
      dim_product_tier_id,
      product_tier_name
    FROM product_tiers
    WHERE product_tier_name = 'SaaS - Trial: Ultimate'

), current_recurring AS (

    SELECT DISTINCT
      fct_mrr_with_zero_dollar_charges.dim_subscription_id,
      product_details.product_rate_plan_id,
      product_details.dim_product_tier_id
    FROM fct_mrr_with_zero_dollar_charges
    INNER JOIN product_details
      ON fct_mrr_with_zero_dollar_charges.dim_product_detail_id = product_details.dim_product_detail_id
      AND product_details.product_deployment_type = 'GitLab.com'
    WHERE fct_mrr_with_zero_dollar_charges.dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
      AND subscription_status IN ('Active', 'Cancelled')

), namespace_list AS (

    SELECT DISTINCT
      namespaces.dim_namespace_id,
      namespaces.namespace_type,
      namespaces.ultimate_parent_namespace_id,
      namespaces.gitlab_plan_id,
      product_tiers.dim_product_tier_id                                 AS dim_product_tier_id_namespace,
      product_tiers.product_tier_name                                   AS product_tier_name_namespace,
      trials.order_start_date                                           AS saas_trial_start_date,
      trials.order_end_date                                             AS saas_trial_expired_on,
      trials.trial_type                                                 AS saas_trial_type,
      trials.trial_type_name                                            AS saas_trial_type_name,
      IFF(trials.dim_namespace_id IS NOT NULL
            OR (namespaces.dim_namespace_id = ultimate_parent_namespace_id
                AND product_tier_name_namespace = 'SaaS - Trial: Ultimate'),
          TRUE, FALSE)                                                  AS namespace_was_trial,
      namespaces.is_currently_valid                                     AS is_namespace_active
    FROM namespaces
    LEFT JOIN product_tiers
      ON namespaces.dim_product_tier_id = product_tiers.dim_product_tier_id
    LEFT JOIN trials
      ON namespaces.dim_namespace_id = trials.dim_namespace_id

), subscription_list AS (

    SELECT DISTINCT
      subscriptions.dim_subscription_id,
      subscriptions.dim_subscription_id_original,
      subscriptions.dim_subscription_id_previous,
      subscriptions.subscription_name,
      subscriptions.subscription_name_slugify,
      subscriptions.dim_billing_account_id,
      subscriptions.dim_crm_account_id,
      subscriptions.subscription_start_date,
      subscriptions.subscription_end_date,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_subscription,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_subscription,
      product_rate_plans.product_tier_name                              AS product_tier_name_subscription,
      COUNT(*) OVER(PARTITION BY subscriptions.dim_subscription_id)     AS count_of_tiers_per_subscription,
      IFF(current_recurring.dim_subscription_id IS NOT NULL,
          TRUE, FALSE)                                                  AS is_subscription_active
    FROM subscriptions
    INNER JOIN saas_subscriptions
      ON subscriptions.dim_subscription_id = saas_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON saas_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON saas_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

), order_list AS (

    SELECT
      orders.internal_order_id                                          AS order_id,
      orders.user_id                                                    AS customer_id,
      COALESCE(trial_tiers.dim_product_tier_id,
               product_rate_plans.dim_product_tier_id)                  AS dim_product_tier_id_with_trial,
      COALESCE(trial_tiers.product_tier_name,
               product_rate_plans.product_tier_name)                    AS product_tier_name_with_trial,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_order,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_order,
      product_rate_plans.product_tier_name                              AS product_tier_name_order,
      orders.dim_subscription_id                                            AS subscription_id_order,
      orders.subscription_name                                          AS subscription_name_order,
      orders.subscription_name_slugify                                  AS subscription_name_slugify_order,
      orders.order_start_date,
      orders.order_end_date,
      orders.dim_namespace_id                                           AS namespace_id_order,
      orders.is_order_trial                                             AS order_is_trial,
      IFF(IFNULL(orders.order_end_date, CURRENT_DATE) >= CURRENT_DATE,
          TRUE, FALSE)                                                  AS is_order_active
    FROM orders
    LEFT JOIN product_rate_plans
      ON orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN trial_tiers
      ON orders.is_order_trial = TRUE
    WHERE orders.order_start_date IS NOT NULL
      AND (product_rate_plans.product_rate_plan_id IS NOT NULL
          OR orders.product_rate_plan_id IN ('premium-saas-trial-plan-id', 'ultimate-saas-trial-plan-id', 'free-plan-id')
          )

), final AS (

    SELECT
      namespace_list.dim_namespace_id,
      subscription_list.dim_subscription_id,
      order_list.order_id,
      order_list.namespace_id_order,
      order_list.subscription_id_order,
      namespace_list.ultimate_parent_namespace_id,
      namespace_list.namespace_type,
      namespace_list.dim_product_tier_id_namespace,
      namespace_list.product_tier_name_namespace,
      namespace_list.is_namespace_active,
      namespace_list.namespace_was_trial,
      namespace_list.saas_trial_start_date,
      namespace_list.saas_trial_expired_on,
      namespace_list.saas_trial_type,
      namespace_list.saas_trial_type_name,
      order_list.customer_id,
      order_list.product_rate_plan_id_order,
      order_list.dim_product_tier_id_order,
      order_list.product_tier_name_order,
      order_list.is_order_active,
      order_list.order_start_date,
      order_list.order_end_date,
      order_list.order_is_trial,
      order_list.dim_product_tier_id_with_trial,
      order_list.product_tier_name_with_trial,
      subscription_list.subscription_name,
      subscription_list.subscription_name_slugify,
      subscription_list.dim_subscription_id_original,
      subscription_list.dim_subscription_id_previous,
      subscription_list.dim_billing_account_id,
      subscription_list.dim_crm_account_id,
      subscription_list.is_subscription_active,
      subscription_list.subscription_start_date,
      subscription_list.subscription_end_date,
      subscription_list.product_rate_plan_id_subscription,
      subscription_list.dim_product_tier_id_subscription,
      subscription_list.product_tier_name_subscription,
      subscription_list.count_of_tiers_per_subscription,
      CASE
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.order_id IS NULL
          THEN 'Trial Namespace Missing Order'
        WHEN order_list.namespace_id_order != namespace_list.ultimate_parent_namespace_id
          AND namespace_list.is_namespace_active = TRUE
          THEN 'Order Linked to Non-Ultimate Parent Namespace'
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND order_list.order_id IS NULL
          THEN 'Paid Namespace Missing Order'
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND order_list.subscription_id_order IS NULL
          THEN 'Paid Namespace Missing Order Subscription'
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Namespace Missing Zuora Subscription'
        WHEN order_list.subscription_id_order IS NOT NULL
          AND namespace_list.dim_namespace_id IS NULL
          THEN 'Paid Order Missing Namespace Assignment'
        WHEN order_list.subscription_id_order IS NOT NULL
          AND order_list.product_rate_plan_id_order IS NOT NULL
          AND subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Order Product Rate Plan Misaligned with Zuora'
        WHEN order_list.order_id IS NOT NULL
          AND order_list.namespace_id_order IS NULL
          THEN 'Free Order Missing Namespace Assignment'
        WHEN order_list.namespace_id_order IS NOT NULL
          AND namespace_list.dim_namespace_id IS NULL
          THEN 'Order Namespace Not Found'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND order_list.order_id IS NULL
          THEN 'Paid Subscription Missing Order'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND namespace_list.dim_namespace_id IS NOT NULL
          THEN 'Paid All Matching'
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.order_id IS NOT NULL
          THEN 'Trial All Matching'
      END                                                               AS namespace_order_subscription_match_status
    FROM order_list
    FULL OUTER JOIN subscription_list
      ON order_list.subscription_id_order = subscription_list.dim_subscription_id
      AND order_list.product_rate_plan_id_order = subscription_list.product_rate_plan_id_subscription
    FULL OUTER JOIN namespace_list
      ON order_list.namespace_id_order = namespace_list.dim_namespace_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@utkarsh060",
    created_date="2021-01-14",
    updated_date="2024-04-12"
) }}
