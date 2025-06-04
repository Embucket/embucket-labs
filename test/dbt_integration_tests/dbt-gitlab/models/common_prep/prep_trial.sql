{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('customers', 'customers_db_customers_source'),
    ('namespaces', 'gitlab_dotcom_namespaces'),
    ('orders_snapshots', 'customers_db_orders_snapshots_base'),
    ('users', 'gitlab_dotcom_users')


]) }},


trials AS (

  SELECT *
  FROM orders_snapshots
  WHERE order_is_trial = TRUE


),

zuora_subscription_with_positive_mrr_tcv AS (

  SELECT DISTINCT
    subscription_name,
    subscription_name_slugify,
    subscription_start_date
  FROM {{ ref('prep_charge') }}
  WHERE tcv > 0
    OR mrr > 0


),

ci_minutes_charges AS (

  SELECT *
  FROM {{ ref('zuora_rate_plan_source') }}
  WHERE rate_plan_name IN ('1,000 CI Minutes', '1,000 Compute Minutes')


),

orders_shapshots_excluding_ci_minutes AS (

  SELECT DISTINCT
    orders_snapshots.order_id,
    orders_snapshots.subscription_name_slugify,
    orders_snapshots.subscription_name,
    orders_snapshots.subscription_id AS dim_subscription_id
  FROM orders_snapshots
  LEFT JOIN ci_minutes_charges
    ON orders_snapshots.subscription_id = ci_minutes_charges.subscription_id
      AND orders_snapshots.product_rate_plan_id = ci_minutes_charges.product_rate_plan_id
  WHERE ci_minutes_charges.subscription_id IS NULL

),

converted_trials AS (

  SELECT DISTINCT
    trials.order_id,
    orders_shapshots_excluding_ci_minutes.dim_subscription_id,
    orders_shapshots_excluding_ci_minutes.subscription_name_slugify,
    orders_shapshots_excluding_ci_minutes.subscription_name,
    subscription.subscription_start_date
  FROM trials
  INNER JOIN orders_shapshots_excluding_ci_minutes
    ON trials.order_id = orders_shapshots_excluding_ci_minutes.order_id
  INNER JOIN zuora_subscription_with_positive_mrr_tcv AS subscription
    ON orders_shapshots_excluding_ci_minutes.subscription_name_slugify = subscription.subscription_name_slugify
      AND orders_shapshots_excluding_ci_minutes.subscription_name = subscription.subscription_name
      AND trials.order_start_date <= subscription.subscription_start_date
  WHERE orders_shapshots_excluding_ci_minutes.subscription_name_slugify IS NOT NULL



),

joined AS (

  SELECT DISTINCT

    trials.order_id                                         AS internal_order_id, --Specific to Customer Dot Orders and can only be joined to CDot Orders
    trials.gitlab_namespace_id                              AS dim_namespace_id,
    trials.product_rate_plan_id,
    customers.customer_id                                   AS internal_customer_id, --Specific to Customer Dot Customers and can only be joined to CDot Customers
    users.user_id,
    IFF(users.user_id IS NOT NULL, TRUE, FALSE)             AS is_gitlab_user,
    users.created_at                                        AS user_created_at,
    namespaces.created_at                                   AS namespace_created_at,
    namespaces.namespace_type,
    IFF(converted_trials.order_id IS NOT NULL, TRUE, FALSE) AS is_trial_converted,
    converted_trials.dim_subscription_id,
    converted_trials.subscription_name_slugify,
    converted_trials.subscription_name,
    converted_trials.subscription_start_date,
    trials.order_created_at,
    trials.order_updated_at,
    trials.trial_type,
    trials.trial_type_name,
    (trials.order_start_date)::DATE                         AS trial_start_date,
    (trials.order_end_date)::DATE                           AS trial_end_date,
    customers.country,
    customers.company_size


  FROM trials
  INNER JOIN customers
    ON trials.customer_id = customers.customer_id
  LEFT JOIN namespaces
    ON trials.gitlab_namespace_id = namespaces.namespace_id
  LEFT JOIN users
    ON customers.customer_provider_user_id = users.user_id
  LEFT JOIN converted_trials
    ON trials.order_id = converted_trials.order_id

  WHERE trial_start_date IS NOT NULL

),

final AS (

  SELECT
    --Primary Key--
    {{ dbt_utils.generate_surrogate_key(['joined.internal_order_id', 'joined.dim_namespace_id', 'joined.dim_subscription_id', 'joined.order_updated_at']) }} AS trial_pk,

    --Natural Key--
    joined.internal_order_id,

    --Foreign Keys--
    joined.dim_namespace_id,
    joined.product_rate_plan_id,
    joined.internal_customer_id,
    joined.user_id,
    joined.dim_subscription_id,

    --Other Attributes
    joined.is_gitlab_user,
    joined.user_created_at,

    joined.namespace_created_at,
    joined.namespace_type,

    joined.is_trial_converted,
    joined.subscription_name,
    joined.subscription_name_slugify,
    joined.subscription_start_date,
    joined.country,
    joined.company_size,

    joined.order_created_at,
    joined.order_updated_at,
    joined.trial_type,
    joined.trial_type_name,
    joined.trial_start_date,
    joined.trial_end_date

  FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@utkarsh060",
    created_date="2024-10-23",
    updated_date="2025-02-03"
) }}
