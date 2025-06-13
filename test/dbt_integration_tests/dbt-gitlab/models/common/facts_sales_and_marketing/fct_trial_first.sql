{{ config(
    tags=["mnpi_exception", "product"]
) }}


WITH first_trial AS (

  SELECT DISTINCT

    internal_order_id,
    dim_namespace_id,
    internal_customer_id,
    FIRST_VALUE(subscription_name) IGNORE NULLS
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at ASC)  AS first_subscription_name_slugify,
    FIRST_VALUE(subscription_name) IGNORE NULLS
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at ASC)  AS first_subscription_name,
    FIRST_VALUE(internal_customer_id)
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at DESC) AS latest_customer_id,
    FIRST_VALUE(dim_namespace_id)
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at DESC) AS latest_namespace_id,
    FIRST_VALUE(user_id)
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at DESC) AS latest_user_id,
    FIRST_VALUE(namespace_type)
      OVER (PARTITION BY internal_order_id ORDER BY order_updated_at DESC) AS latest_namespace_type,
    FIRST_VALUE(company_size)
      OVER (PARTITION BY internal_order_id ORDER BY user_created_at DESC) AS latest_company_size,
    FIRST_VALUE(namespace_type)
      OVER (PARTITION BY internal_order_id ORDER BY user_created_at DESC) AS latest_country,
    namespace_created_at,
    user_created_at,
    order_created_at,
    order_updated_at,
    trial_start_date,
    trial_end_date,
    subscription_start_date,
    is_trial_converted

  FROM {{ ref('fct_trial') }}
  WHERE trial_start_date >= '2019-09-01' --The `customers_db_orders_snapshots_base` model has reliable data from the 1st of September, 2019, therefore only the orders that have a `start_date` after this date are included.

), final AS (

  SELECT

   --Primary Key--
     {{ dbt_utils.generate_surrogate_key(['first_trial.internal_order_id', 'first_trial.latest_namespace_id']) }} AS trial_first_pk,

   --Natural Key--
    first_trial.internal_order_id                                                                        AS internal_order_id, --Can only be used to join to CDot Orders

    --Foreign Keys--
    first_trial.latest_namespace_id                                                                      AS dim_namespace_id,
    first_trial.latest_user_id                                                                           AS user_id,
    first_trial.latest_customer_id                                                                       AS internal_customer_id, --Can only be used to join to CDot Customers

    --Other Attributes
    first_trial.first_subscription_name                                                                  AS subscription_name,
    first_trial.first_subscription_name_slugify                                                          AS subscription_name_slugify,
    first_trial.latest_namespace_type                                                                    AS namespace_type,
    first_trial.is_trial_converted                                                                       AS is_trial_converted,
    IFF(first_trial.latest_user_id IS NOT NULL, TRUE, FALSE)                                             AS is_gitlab_user,
    first_trial.latest_company_size                                                                      AS company_size,
    first_trial.latest_country                                                                           AS country,
    MIN(first_trial.subscription_start_date)                                                             AS subscription_start_date,
    MIN(first_trial.user_created_at)                                                                     AS user_created_at,
    MIN(first_trial.namespace_created_at)                                                                AS namespace_created_at,
    MIN(first_trial.order_created_at)                                                                    AS order_created_at,
    MIN(first_trial.trial_start_date)                                                                    AS trial_start_date,
    MAX(first_trial.trial_end_date)                                                                      AS trial_end_date

  FROM first_trial
 {{dbt_utils.group_by(12)}}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-07-18",
    updated_date="2023-07-18"
) }}
