WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_orders') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                              AS order_id,
      customer_id::NUMBER                                     AS customer_id,
      billing_account_id::NUMBER                              AS billing_account_id,
      product_rate_plan_id::VARCHAR                           AS product_rate_plan_id,
      subscription_id::VARCHAR                                AS subscription_id,
      subscription_name::VARCHAR                              AS subscription_name,
      {{zuora_slugify("subscription_name")}}::VARCHAR         AS subscription_name_slugify,
      start_date::TIMESTAMP                                   AS order_start_date,
      end_date::TIMESTAMP                                     AS order_end_date,
      quantity::NUMBER                                        AS order_quantity,
      created_at::TIMESTAMP                                   AS order_created_at,
      updated_at::TIMESTAMP                                   AS order_updated_at,
      TRY_TO_DECIMAL(NULLIF(gl_namespace_id, ''))::VARCHAR    AS gitlab_namespace_id,
      NULLIF(gl_namespace_name, '')::VARCHAR                  AS gitlab_namespace_name,
      amendment_type::VARCHAR                                 AS amendment_type,
      trial::BOOLEAN                                          AS order_is_trial,
      last_extra_ci_minutes_sync_at::TIMESTAMP                AS last_extra_ci_minutes_sync_at,
      zuora_account_id::VARCHAR                               AS zuora_account_id,
      increased_billing_rate_notified_at::TIMESTAMP           AS increased_billing_rate_notified_at,
      source::VARCHAR                                         AS order_source,
      _task_instance::VARCHAR                                 AS task_instance
    FROM source

)

SELECT
  *
FROM renamed
