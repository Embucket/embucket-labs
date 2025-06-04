{{
  config(
    tags=["product", "mnpi_exception"],
    schema="legacy"
  )
}}

WITH customers_db_license_seat_links AS (

    SELECT *
    FROM {{ ref('prep_usage_self_managed_seat_link') }}

), customers_db_orders AS (

    SELECT *
    FROM {{ ref('prep_order') }}

), gitlab_dotcom_gitlab_subscriptions AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions') }}

), gitlab_dotcom_memberships AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_memberships') }}

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription') }}

), rate_plans AS (

    SELECT
      subscription_id,
      ARRAY_AGG(DISTINCT delivery) AS delivery
    FROM zuora_rate_plan
    WHERE amendement_type != 'RemoveProduct'
    GROUP BY 1

), subscriptions AS (

    SELECT
      original_id,
      subscription_id,
      subscription_name,
      subscription_status,
      subscription_start_date::DATE AS subscription_start_date,
      subscription_end_date::DATE   AS subscription_end_date
    FROM zuora_subscription
    WHERE original_id IS NOT NULL
      AND subscription_status IN ('Active', 'Cancelled')

), zuora AS (

    SELECT
      subscriptions.*,
      rate_plans.delivery,
      CASE
        WHEN ARRAY_CONTAINS('Self-Managed'::VARIANT, delivery) THEN 'Self-Managed'
        WHEN ARRAY_CONTAINS('SaaS'::VARIANT, delivery) THEN 'SaaS'
      ELSE 'Others' END AS delivery_group
    FROM subscriptions
    INNER JOIN rate_plans
      ON subscriptions.subscription_id = rate_plans.subscription_id

), zuora_minus_exceptions AS (

    SELECT *
    FROM zuora
    QUALIFY COUNT(*) OVER (PARTITION BY subscription_name) = 1

), seat_link AS (

    SELECT *
    FROM customers_db_license_seat_links
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_name ORDER BY report_date DESC) = 1

), self_managed AS (

    SELECT
      zuora_minus_exceptions.subscription_name,
      zuora_minus_exceptions.original_id,
      zuora_minus_exceptions.subscription_id,
      zuora_minus_exceptions.subscription_status,
      seat_link.report_date,
      seat_link.active_user_count,
      seat_link.max_historical_user_count,
      seat_link.license_user_count
    FROM zuora_minus_exceptions
    LEFT JOIN seat_link
      ON zuora_minus_exceptions.subscription_name = seat_link.subscription_name
    WHERE zuora_minus_exceptions.delivery_group = 'Self-Managed'

), orders AS (

    SELECT
      subscription_name,
      dim_subscription_id,
      product_rate_plan_id,
      dim_namespace_id,
      order_start_date,
      order_end_date,
      order_updated_at
    FROM customers_db_orders
    WHERE dim_namespace_id IS NOT NULL
      AND is_order_trial = FALSE
      AND order_end_date > CURRENT_DATE

), latest_order_per_subscription_name AS (

    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY subscription_name
        ORDER BY order_end_date DESC, order_updated_at DESC
    ) = 1

), customers AS (

    SELECT
      zuora_minus_exceptions.*,
      latest_order_per_subscription_name.dim_namespace_id
    FROM zuora_minus_exceptions
    LEFT JOIN latest_order_per_subscription_name
      ON zuora_minus_exceptions.subscription_name = latest_order_per_subscription_name.subscription_name
    WHERE delivery_group = 'SaaS'

), customers_minus_exceptions AS (

    SELECT *
    FROM customers
    QUALIFY COUNT(*) OVER (PARTITION BY subscription_id) = 1

), gitlab_subscriptions AS (

    SELECT
      namespace_id,
      max_seats_used AS max_historical_user_count,
      seats          AS license_user_count
    FROM gitlab_dotcom_gitlab_subscriptions
    WHERE is_currently_valid = TRUE

), membership AS (

    SELECT
      ultimate_parent_id                                            AS namespace_id,
      COUNT(DISTINCT CASE WHEN is_billable = TRUE THEN user_id END) AS active_user_count
    FROM gitlab_dotcom_memberships
    GROUP BY 1

), saas_seats AS (

    SELECT
      gitlab_subscriptions.namespace_id,
      gitlab_subscriptions.max_historical_user_count,
      gitlab_subscriptions.license_user_count,
      membership.active_user_count
    FROM gitlab_subscriptions
    LEFT JOIN membership
      ON gitlab_subscriptions.namespace_id = membership.namespace_id

), saas AS (

    SELECT
      customers_minus_exceptions.subscription_name,
      customers_minus_exceptions.original_id,
      customers_minus_exceptions.subscription_id,
      customers_minus_exceptions.subscription_status,
      CURRENT_DATE() AS report_date,
      saas_seats.active_user_count,
      saas_seats.max_historical_user_count,
      saas_seats.license_user_count
    FROM customers_minus_exceptions
    LEFT JOIN saas_seats
      ON customers_minus_exceptions.dim_namespace_id = saas_seats.namespace_id

), final AS (

    SELECT
      'Self-Managed' AS delivery_group,
      self_managed.*
    FROM self_managed

    UNION

    SELECT
      'SaaS' AS delivery_group,
      saas.*
    FROM saas

)

SELECT *
FROM final
