{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('subscriptions', 'prep_subscription'),
    ('orders', 'customers_db_orders_source'),
    ('product_tiers', 'prep_product_tier'),
    ('product_details', 'dim_product_detail'),
    ('fct_mrr_with_zero_dollar_charges', 'fct_mrr_with_zero_dollar_charges'),
    ('subscription_delivery_types', 'bdg_subscription_product_rate_plan')
]) }}

, trial_tiers AS (

    SELECT
      dim_product_tier_id,
      product_tier_name
    FROM product_tiers
    WHERE product_tier_name = 'Self-Managed - Trial: Ultimate'

), product_rate_plans AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name
    FROM product_details
    WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')

), sm_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      product_rate_plan_id,
      dim_product_tier_id
    FROM subscription_delivery_types
    WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')

), current_recurring AS (

    SELECT DISTINCT
      fct_mrr_with_zero_dollar_charges.dim_subscription_id,
      product_details.product_rate_plan_id,
      product_details.dim_product_tier_id
    FROM fct_mrr_with_zero_dollar_charges
    INNER JOIN product_details
      ON fct_mrr_with_zero_dollar_charges.dim_product_detail_id = product_details.dim_product_detail_id
      AND product_details.product_deployment_type IN ('Self-Managed', 'Dedicated')
    WHERE fct_mrr_with_zero_dollar_charges.dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
      AND subscription_status IN ('Active', 'Cancelled')

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
    INNER JOIN sm_subscriptions
      ON subscriptions.dim_subscription_id = sm_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON sm_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON sm_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

), order_list AS (

    SELECT
      orders.order_id,
      orders.customer_id,
      IFNULL(trial_tiers.dim_product_tier_id,
              product_rate_plans.dim_product_tier_id)                   AS dim_product_tier_id_with_trial,
      IFNULL(trial_tiers.product_tier_name,
              product_rate_plans.product_tier_name)                     AS product_tier_name_with_trial,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_order,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_order,
      product_rate_plans.product_tier_name                              AS product_tier_name_order,
      orders.subscription_id                                            AS subscription_id_order,
      orders.subscription_name                                          AS subscription_name_order,
      orders.subscription_name_slugify                                  AS subscription_name_slugify_order,
      orders.order_start_date,
      orders.order_end_date,
      orders.order_is_trial,
      IFF(IFNULL(orders.order_end_date, CURRENT_DATE) >= CURRENT_DATE,
          TRUE, FALSE)                                                  AS is_order_active
    FROM orders
    INNER JOIN product_rate_plans
      ON orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN trial_tiers
      ON orders.order_is_trial = TRUE
    WHERE orders.order_start_date IS NOT NULL

), final AS (

    SELECT
      subscription_list.dim_subscription_id,
      order_list.order_id,
      order_list.subscription_id_order,
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
      CASE
        WHEN order_list.product_tier_name_with_trial = 'Self-Managed - Trial: Ultimate'
          THEN 'Trial Order'
        WHEN order_list.order_id IS NOT NULL
          AND order_list.subscription_id_order IS NULL
          THEN 'Order Subscription Not Found'
        WHEN order_list.subscription_id_order IS NOT NULL
          AND subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Order Product Rate Plan Misaligned with Zuora'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND order_list.order_id IS NULL
          THEN 'Paid Subscription Missing Order'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND order_list.order_id IS NOT NULL
          THEN 'Paid All Matching'
      END                                                               AS order_subscription_match_status
    FROM order_list
    FULL OUTER JOIN subscription_list
      ON order_list.subscription_id_order = subscription_list.dim_subscription_id
      AND order_list.product_rate_plan_id_order = subscription_list.product_rate_plan_id_subscription

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@jpeguero",
    created_date="2021-02-02",
    updated_date="2022-06-22"
) }}
