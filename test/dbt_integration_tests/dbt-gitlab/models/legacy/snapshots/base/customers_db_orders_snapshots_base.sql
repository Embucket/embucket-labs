{{ config({
    "alias": "customers_db_orders_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'customers_db_orders_snapshots') }}

), renamed AS (

    SELECT 
      dbt_scd_id::VARCHAR                                     AS order_snapshot_id,
      id::NUMBER                                              AS order_id,
      customer_id::NUMBER                                     AS customer_id,
      product_rate_plan_id::VARCHAR                           AS product_rate_plan_id,
      billing_account_id::VARCHAR                             AS billing_account_id,
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

      -- Map trial types for all trials, including historical data per this mapping: https://gitlab.com/gitlab-data/analytics/-/issues/21199#trial-type-to-product-mapping
      -- Context:
        -- 1. trial_type column was added to the customer_db_orders table on 2024-12-13
        -- 2. Engineering team performed a backfill for the trial_type column, but snapshot records still require manual mapping to map subsequent changes to the trial_type column
        -- 3. For ex - Free trials (product_rate_plan_id = 'free-plan-id') initially had NULL trial_type
        -- 4. When these trials converted to paid plans, the trial_type remained NULL in the snapshot
        -- 5. This manual mapping ensures consistency across all historical and current data, addressing gaps left by the backfill process

      CASE 
        WHEN product_rate_plan_id = 'ultimate-saas-trial-plan-id' THEN 1
        WHEN product_rate_plan_id = 'saas-gitlab-duo-pro-trial-plan-id' THEN 2
        WHEN product_rate_plan_id = 'saas-gitlab-duo-enterprise-trial-plan-id' THEN 3
        WHEN product_rate_plan_id = 'ultimate-saas-trial-paid-customer-plan-id' THEN 4
        WHEN product_rate_plan_id = 'ultimate-saas-trial-w-duo-enterprise-trial-plan-id' THEN 5
        WHEN product_rate_plan_id = 'ultimate-saas-trial-paid-customer-w-duo-enterprise-trial-plan-id' THEN 6
        WHEN product_rate_plan_id = 'premium-saas-trial-plan-id' THEN 7
        WHEN trial_type IS NOT NULL THEN trial_type  -- Preserve existing non-NULL trial_types
        ELSE NULL  -- Handle any other scenarios
      END::INTEGER                                            AS trial_type,

      CASE 
        WHEN product_rate_plan_id = 'ultimate-saas-trial-plan-id' THEN 'Ultimate/Premium'
        WHEN product_rate_plan_id = 'saas-gitlab-duo-pro-trial-plan-id' THEN 'DuoPro'
        WHEN product_rate_plan_id IN ('saas-gitlab-duo-enterprise-trial-plan-id', 
                                      'ultimate-saas-trial-w-duo-enterprise-trial-plan-id', 
                                      'ultimate-saas-trial-paid-customer-w-duo-enterprise-trial-plan-id') THEN 'Duo Enterprise'
        WHEN product_rate_plan_id = 'ultimate-saas-trial-paid-customer-plan-id' THEN 'Ultimate on Premium'
        WHEN product_rate_plan_id = 'premium-saas-trial-plan-id' THEN 'Premium'
        ELSE NULL  -- Handle any other scenarios
      END                                                     AS trial_type_name,
      last_extra_ci_minutes_sync_at::TIMESTAMP                AS last_extra_ci_minutes_sync_at,
      "DBT_VALID_FROM"::TIMESTAMP                             AS valid_from,
      "DBT_VALID_TO"::TIMESTAMP                               AS valid_to
    FROM source

)

SELECT *
FROM renamed
