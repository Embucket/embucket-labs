{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('namespaces', 'prep_namespace'),
    ('subscriptions', 'prep_subscription'),
    ('orders_historical', 'dim_order_hist'),
    ('dates', 'dim_date'),
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
      fct_mrr_with_zero_dollar_charges.dim_subscription_id
    FROM fct_mrr_with_zero_dollar_charges
    INNER JOIN product_details
      ON fct_mrr_with_zero_dollar_charges.dim_product_detail_id = product_details.dim_product_detail_id
    WHERE fct_mrr_with_zero_dollar_charges.dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
      AND product_details.product_deployment_type = 'GitLab.com'
      AND subscription_status IN ('Active', 'Cancelled')

), namespace_list AS (

    SELECT DISTINCT
      namespaces.dim_namespace_id,
      namespaces.namespace_type,
      namespaces.ultimate_parent_namespace_id,
      namespaces.gitlab_plan_id,
      dates.first_day_of_month                                          AS namespace_snapshot_month,
      trials.order_start_date                                           AS saas_trial_start_date,
      trials.order_end_date                                             AS saas_trial_expired_on,
      trials.trial_type                                                 AS saas_trial_type,
      trials.trial_type_name                                            AS saas_trial_type_name,
      IFF(trials.dim_namespace_id IS NOT NULL
            OR (namespaces.dim_namespace_id = ultimate_parent_namespace_id
                AND namespaces.gitlab_plan_title = 'Ultimate Trial'),
          TRUE, FALSE)                                                  AS namespace_was_trial,
      namespaces.is_currently_valid                                     AS is_namespace_active
    FROM namespaces
    INNER JOIN dates
      ON dates.date_actual BETWEEN namespaces.created_at AND CURRENT_DATE
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
      subscriptions.subscription_version,
      dates.first_day_of_month                                          AS subscription_snapshot_month,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_subscription,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_subscription,
      product_rate_plans.product_tier_name                              AS product_tier_name_subscription,
      IFF(current_recurring.dim_subscription_id IS NOT NULL,
          TRUE, FALSE)                                                  AS is_subscription_active
    FROM subscriptions
    INNER JOIN dates
      ON dates.date_actual BETWEEN subscriptions.subscription_start_date
                            AND IFNULL(subscriptions.subscription_end_date, CURRENT_DATE)
    INNER JOIN saas_subscriptions
      ON subscriptions.dim_subscription_id = saas_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON saas_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

), orders AS (
    /*
    This CTE transforms orders from the historical orders table in two significant ways:
      1. It corrects for erroneous order start/end dates by substituting in the valid_from/valid_to columns
          when changes are made to the order (generally remapping to renewed subscriptions, new namespaces)
        a. See term_start_date and term_end_date (identifiers borrowed from the Zuora subscription model)
      2. It smooths over same day updates to the namespace linked to a given order,
          which would otherwise result in multiple rows for an order in a given month
        a. See QUALIFY statement below. This gets the last update to an order on a given day
        b. NOTE: This does remove some order-namespace links that existed in the historical orders table
            at one point in time, but a judgement call was made to assume that if the namespace needed
            to be updated within 24 hours it is likely that the previous namespace was incorrect
    */
    SELECT
      orders_historical.dim_order_id,
      orders_historical.customer_id,
      IFNULL(trial_tiers.dim_product_tier_id,
              product_rate_plans.dim_product_tier_id)                   AS dim_product_tier_id_with_trial,
      IFNULL(trial_tiers.product_tier_name,
              product_rate_plans.product_tier_name)                     AS product_tier_name_with_trial,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_order,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_order,
      product_rate_plans.product_tier_name                              AS product_tier_name_order,
      orders_historical.dim_subscription_id                             AS subscription_id_order,
      orders_historical.dim_namespace_id                                AS namespace_id_order,
      MIN(orders_historical.order_start_date) OVER(
        PARTITION BY orders_historical.dim_order_id)                    AS order_start_date,
      MAX(orders_historical.order_end_date) OVER(
        PARTITION BY orders_historical.dim_order_id)                    AS order_end_date,
      MIN(orders_historical.valid_from) OVER (
        PARTITION BY
          orders_historical.dim_order_id,
          orders_historical.dim_subscription_id,
          orders_historical.dim_namespace_id)                           AS term_start_date,
      MAX(IFNULL(orders_historical.valid_to, CURRENT_DATE)) OVER (
        PARTITION BY
          orders_historical.dim_order_id,
          orders_historical.dim_subscription_id,
          orders_historical.dim_namespace_id)                           AS term_end_date,
      orders_historical.order_is_trial,
      IFF(order_end_date >= CURRENT_DATE,
          TRUE, FALSE)                                                  AS is_order_active
    FROM orders_historical
    INNER JOIN product_rate_plans
      ON orders_historical.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN trial_tiers
      ON orders_historical.order_is_trial = TRUE
    WHERE order_start_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        orders_historical.dim_order_id,
        orders_historical.valid_from::DATE
      ORDER BY orders_historical.valid_from DESC
    ) = 1

), order_list AS (

    SELECT
      orders.*,
      dates.first_day_of_month                                          AS order_snapshot_month
    FROM orders
    INNER JOIN dates
      ON dates.date_actual BETWEEN IFF(orders.term_start_date < orders.order_start_date,
                                       orders.order_start_date, orders.term_start_date)
                            AND IFF(orders.term_end_date > orders.order_end_date,
                                    orders.order_end_date, orders.term_end_date)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        orders.dim_order_id,
        dates.first_day_of_month
      ORDER BY orders.term_end_date DESC
    ) = 1

), final AS (

    SELECT DISTINCT
      namespace_list.dim_namespace_id,
      subscription_list.dim_subscription_id,
      order_list.dim_order_id,
      COALESCE(order_list.order_snapshot_month,
               subscription_list.subscription_snapshot_month,
               namespace_list.namespace_snapshot_month
              )                                                         AS snapshot_month,
      order_list.namespace_id_order,
      order_list.subscription_id_order,
      namespace_list.ultimate_parent_namespace_id,
      namespace_list.namespace_type,
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
      subscription_list.subscription_version,
      CASE
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.dim_order_id IS NULL
          THEN 'Trial Namespace Missing Order'
        WHEN order_list.namespace_id_order != namespace_list.ultimate_parent_namespace_id
          AND namespace_list.is_namespace_active = TRUE
          THEN 'Order Linked to Non-Ultimate Parent Namespace'
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND order_list.dim_order_id IS NULL
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
        WHEN order_list.dim_order_id IS NOT NULL
          AND order_list.namespace_id_order IS NULL
          THEN 'Free Order Missing Namespace Assignment'
        WHEN order_list.namespace_id_order IS NOT NULL
          AND namespace_list.dim_namespace_id IS NULL
          THEN 'Order Namespace Not Found'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND order_list.dim_order_id IS NULL
          THEN 'Paid Subscription Missing Order'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND namespace_list.dim_namespace_id IS NOT NULL
          THEN 'Paid All Matching'
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.dim_order_id IS NOT NULL
          THEN 'Trial All Matching'
      END                                                               AS namespace_order_subscription_match_status
    FROM order_list
    FULL OUTER JOIN subscription_list
      ON order_list.subscription_id_order = subscription_list.dim_subscription_id
      AND order_list.product_rate_plan_id_order = subscription_list.product_rate_plan_id_subscription
      AND order_list.order_snapshot_month = subscription_list.subscription_snapshot_month
    FULL OUTER JOIN namespace_list
      ON order_list.namespace_id_order = namespace_list.dim_namespace_id
      AND order_list.order_snapshot_month = namespace_list.namespace_snapshot_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@utkarsh060",
    created_date="2021-06-02",
    updated_date="2024-04-12"
) }}
