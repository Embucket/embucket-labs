{{ config(
    tags=["mnpi_exception"]
) }}


WITH trial_histories AS (

  SELECT

    gl_namespace_id,
    start_date,
    expired_on,
    created_at,
    updated_at,
    glm_source,
    glm_content,
    trial_type,
    trial_type_name,
    'trial_histories' AS record_source

  FROM {{ ref('customers_db_trial_histories_source') }}


),

trial_orders AS (

  SELECT

    gitlab_namespace_id,
    order_start_date,
    order_end_date,
    order_created_at,
    order_updated_at,
    order_source,
    subscription_id AS dim_subscription_id,
    product_rate_plan_id,
    'orders'        AS record_source

  FROM {{ ref('customers_db_orders_source') }}

  WHERE order_is_trial = TRUE
    AND gitlab_namespace_id IS NOT NULL
    AND gitlab_namespace_id
    NOT IN
    (SELECT gl_namespace_id FROM {{ ref('customers_db_trial_histories_source') }})
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(gitlab_namespace_id) ORDER BY order_updated_at DESC) = 1


),

final AS (

  SELECT

    --Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['COALESCE(trial_histories.gl_namespace_id, trial_orders.gitlab_namespace_id)','trial_histories.trial_type']) }} AS dim_namespace_order_trial_sk,

    --Natural Key
    COALESCE(trial_histories.gl_namespace_id, trial_orders.gitlab_namespace_id)                                                                                                                                                                             AS dim_namespace_id,

    --Other Attributes  
    COALESCE(trial_histories.start_date, trial_orders.order_start_date)                                                                                                                                                                                     AS order_start_date,
    COALESCE(trial_histories.expired_on, trial_orders.order_end_date)                                                                                                                                                                                       AS order_end_date,
    COALESCE(trial_histories.created_at, trial_orders.order_created_at)                                                                                                                                                                                     AS order_created_at,
    COALESCE(trial_histories.updated_at, trial_orders.order_updated_at)                                                                                                                                                                                     AS order_updated_at,
    COALESCE(trial_histories.glm_source, trial_orders.order_source)                                                                                                                                                                                         AS glm_source,
    trial_orders.dim_subscription_id,
    trial_orders.product_rate_plan_id,
    trial_histories.glm_content,
    trial_histories.trial_type,
    trial_histories.trial_type_name,
    COALESCE(trial_histories.record_source, trial_orders.record_source)                                                                                                                                                                                     AS record_source


  FROM trial_histories
  FULL JOIN trial_orders
    ON trial_histories.gl_namespace_id = trial_orders.gitlab_namespace_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@rakreddy",
    created_date="2023-06-19",
    updated_date="2024-12-13"
) }}
