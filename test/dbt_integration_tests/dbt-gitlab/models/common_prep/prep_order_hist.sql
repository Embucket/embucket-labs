{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('versions', 'customers_db_versions_source'),
    ('current_orders', 'customers_db_orders_source'),
    ('dim_date', 'dim_date'),
    ('namespaces', 'prep_namespace'),
    ('subscriptions', 'dim_subscription'),
    ('billing_accounts', 'dim_billing_account')
]) }}

, customers_db_versions AS (

    SELECT *
    FROM versions
    -- selecting only orders
    WHERE item_type = 'Order'
      AND object IS NOT NULL

), flattened_object AS (

    -- objects look like a yaml table, splitting the object into rows at each linebreak
    -- column keys will be turned into column names and populated by the associated column values
    -- column values are all strings, some wrapped in extra quotations, some containing multiple colons 
    SELECT
      *,
      SPLIT_PART(value, ': ', 1)                                                          AS column_key,
      NULLIF(TRIM(SPLIT_PART(value, column_key || ': ', 2), ''''),'')                     AS column_value
    FROM customers_db_versions,
    LATERAL SPLIT_TO_TABLE(object, '\n')

), cleaned AS (

    -- this CTE attempts to further clean up column values
    -- namespace id: messy data from source, uses regular expression to remove all non-numeric characters
    -- boolean column: set NULL equal to FALSE
    -- timestamp columns: can come with 3-4 additional rows in the original object
    --   when the associated column_value for each timestamp column_key is not a timestamp the 3rd or 4th
    --   row following contains the actual timestamp value
    --   additionally, the created_at column sometimes contained '&1 ' before the timestamp value
    SELECT
      version_id,
      item_id                                                                             AS order_id,
      created_at                                                                          AS valid_to,
      IFF(column_key = 'customer_id', column_value::NUMBER, NULL)                         AS customer_id,
      IFF(column_key = 'product_rate_plan_id', column_value, NULL)                        AS product_rate_plan_id,
      IFF(column_key = 'subscription_id', column_value, NULL)                             AS subscription_id,
      IFF(column_key = 'subscription_name', column_value, NULL)                           AS subscription_name,
      IFF(column_key = 'start_date', column_value::DATE, NULL)                            AS order_start_date,
      IFF(column_key = 'end_date', column_value::DATE, NULL)                              AS order_end_date,
      IFF(column_key = 'quantity', column_value::NUMBER, NULL)                            AS order_quantity,
      IFF(column_key = 'created_at',
          COALESCE(TRY_TO_TIMESTAMP(LTRIM(column_value, '&1 ')),
                   TRY_TO_TIMESTAMP(LAG(column_value, 3)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC)),
                   TRY_TO_TIMESTAMP(LAG(column_value, 4)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC))),
          NULL)                                                                           AS order_created_at,
      IFF(column_key = 'updated_at',
          COALESCE(TRY_TO_TIMESTAMP(column_value),
                   TRY_TO_TIMESTAMP(LAG(column_value, 3)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC)),
                   TRY_TO_TIMESTAMP(LAG(column_value, 4)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC))),
          NULL)                                                                           AS order_updated_at,
      IFF(column_key = 'gl_namespace_id',
          TRY_TO_NUMBER(REGEXP_REPLACE(column_value, '[^0-9]+', '')),
          NULL)                                                                           AS gitlab_namespace_id,
      IFF(column_key = 'gl_namespace_name', column_value, NULL)                           AS gitlab_namespace_name,
      IFF(column_key = 'amendment_type', column_value, NULL)                              AS amendment_type,
      IFF(column_key = 'trial', IFNULL(column_value, FALSE)::BOOLEAN, NULL)               AS order_is_trial,
      IFF(column_key = 'last_extra_ci_minutes_sync_at',
          COALESCE(TRY_TO_TIMESTAMP(column_value),
                   TRY_TO_TIMESTAMP(LAG(column_value, 3)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC)),
                   TRY_TO_TIMESTAMP(LAG(column_value, 4)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC))),
          NULL)                                                                           AS last_extra_ci_minutes_sync_at,
      IFF(column_key = 'zuora_account_id', column_value, NULL)                            AS zuora_account_id,
      IFF(column_key = 'increased_billing_rate_notified_at',
          COALESCE(TRY_TO_TIMESTAMP(column_value),
                   TRY_TO_TIMESTAMP(LAG(column_value, 3)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC)),
                   TRY_TO_TIMESTAMP(LAG(column_value, 4)
                     OVER (PARTITION BY version_id, item_id, seq ORDER BY index DESC))),
          NULL)                                                                           AS increased_billing_rate_notified_at
    FROM flattened_object
  
), pivoted AS (

    SELECT 
      version_id, 
      order_id,
      valid_to,
      MAX(customer_id)                                                                    AS customer_id,
      MAX(product_rate_plan_id)                                                           AS product_rate_plan_id,
      MAX(subscription_id)                                                                AS subscription_id,
      MAX(subscription_name)                                                              AS subscription_name,
      MAX(order_start_date)                                                               AS order_start_date,
      MAX(order_end_date)                                                                 AS order_end_date,
      MAX(order_quantity)                                                                 AS order_quantity,
      MAX(order_created_at)                                                               AS order_created_at,
      MAX(order_updated_at)                                                               AS order_updated_at,
      MAX(gitlab_namespace_id)                                                            AS gitlab_namespace_id,
      MAX(gitlab_namespace_name)                                                          AS gitlab_namespace_name,
      MAX(amendment_type)                                                                 AS amendment_type,
      MAX(order_is_trial)                                                                 AS order_is_trial,
      MAX(last_extra_ci_minutes_sync_at)                                                  AS last_extra_ci_minutes_sync_at,
      MAX(zuora_account_id)                                                               AS zuora_account_id,
      MAX(increased_billing_rate_notified_at)                                             AS increased_billing_rate_notified_at
    FROM cleaned
    {{ dbt_utils.group_by(n=3) }}

), unioned AS (

    SELECT 
      order_id                                                                            AS dim_order_id,
      customer_id,
      product_rate_plan_id,
      subscription_id                                                                     AS dim_subscription_id,
      subscription_name,
      order_start_date,
      order_end_date,
      order_quantity,
      order_created_at,
      gitlab_namespace_id::NUMBER                                                         AS dim_namespace_id,
      gitlab_namespace_name                                                               AS namespace_name,
      amendment_type,
      order_is_trial,
      last_extra_ci_minutes_sync_at,
      zuora_account_id                                                                    AS dim_billing_account_id,
      increased_billing_rate_notified_at,
      IFNULL(LAG(valid_to) OVER (PARTITION BY order_id ORDER BY version_id),
             order_created_at)                                                            AS valid_from,
      valid_to
    FROM pivoted
    WHERE order_created_at IS NOT NULL

    UNION ALL

    SELECT 
      order_id,
      customer_id,
      product_rate_plan_id,
      subscription_id,
      subscription_name,
      order_start_date,
      order_end_date,
      order_quantity,
      order_created_at,
      gitlab_namespace_id::NUMBER,
      gitlab_namespace_name,
      amendment_type,
      order_is_trial,
      last_extra_ci_minutes_sync_at,
      zuora_account_id,
      increased_billing_rate_notified_at,
      order_updated_at                                                                    AS valid_from,
      NULL                                                                                AS valid_to
    FROM current_orders

), joined AS (

    SELECT 
      unioned.dim_order_id,
      unioned.customer_id,
      unioned.product_rate_plan_id,
      unioned.order_created_at,
      start_dates.date_day                                                                AS order_start_date,
      end_dates.date_day                                                                  AS order_end_date,
      unioned.order_quantity,
      subscriptions.dim_subscription_id,
      subscriptions.subscription_name,
      namespaces.dim_namespace_id,
      namespaces.namespace_name,
      billing_accounts.dim_billing_account_id,
      unioned.amendment_type,
      unioned.order_is_trial,
      unioned.last_extra_ci_minutes_sync_at,
      unioned.increased_billing_rate_notified_at,
      unioned.valid_from,
      unioned.valid_to
    FROM unioned
    LEFT JOIN subscriptions
      ON unioned.dim_subscription_id = subscriptions.dim_subscription_id
    LEFT JOIN namespaces
      ON unioned.dim_namespace_id = namespaces.dim_namespace_id
    LEFT JOIN billing_accounts
      ON unioned.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN dim_date AS start_dates
      ON unioned.order_start_date = start_dates.date_day
    LEFT JOIN dim_date AS end_dates
      ON unioned.order_end_date = end_dates.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-07-07",
    updated_date="2021-07-07"
) }}
