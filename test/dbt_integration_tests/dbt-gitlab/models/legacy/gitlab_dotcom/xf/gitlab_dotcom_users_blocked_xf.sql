{{ config(
    tags=["mnpi_exception"]
) }}

WITH customers AS (

    SELECT *
    FROM {{ ref('prep_user_trial') }}

), trials AS  (

    SELECT *
    FROM {{ ref('fct_trial_first') }}

), users AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_users')) }},
      created_at AS user_created_at,
      updated_at AS user_updated_at
    FROM {{ ref('gitlab_dotcom_users') }}

), highest_paid_subscription_plan AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_highest_paid_subscription_plan') }}

),   customers_with_trial AS (

    SELECT
      customers.user_provider_user_id                             AS user_id,
      MIN(customers.dim_user_id)                                  AS first_customer_id,
      MIN(customers.user_created_at)                              AS first_customer_created_at,
      ARRAY_AGG(customers.dim_user_id)
          WITHIN GROUP (ORDER  BY customers.dim_user_id)          AS customer_id_list,
      MAX(IFF(trials.internal_order_id IS NOT NULL, TRUE, FALSE)) AS has_started_trial,
      MIN(trials.trial_start_date)                                AS has_started_trial_at
    FROM customers
      LEFT JOIN trials
        ON customers.dim_user_id = trials.internal_customer_id
    WHERE customers.user_provider = 'gitlab'
    GROUP BY 1

),   joined AS (
    SELECT
      users.*,
      TIMESTAMPDIFF(DAYS, user_created_at, last_activity_on)                       AS days_active,
      TIMESTAMPDIFF(DAYS, user_created_at, CURRENT_TIMESTAMP(2))                   AS account_age,
      CASE
        WHEN account_age <= 1 THEN '1 - 1 day or less'
        WHEN account_age <= 7 THEN '2 - 2 to 7 days'
        WHEN account_age <= 14 THEN '3 - 8 to 14 days'
        WHEN account_age <= 30 THEN '4 - 15 to 30 days'
        WHEN account_age <= 60 THEN '5 - 31 to 60 days'
        WHEN account_age > 60 THEN '6 - Over 60 days'
      END                                                                           AS account_age_cohort,

      highest_paid_subscription_plan.highest_paid_subscription_plan_id,
      highest_paid_subscription_plan.highest_paid_subscription_plan_is_paid         AS is_paid_user,
      highest_paid_subscription_plan.highest_paid_subscription_namespace_id,
      highest_paid_subscription_plan.highest_paid_subscription_ultimate_parent_id,
      highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_type,
      highest_paid_subscription_plan.highest_paid_subscription_inheritance_source_id,

      IFF(customers_with_trial.first_customer_id IS NOT NULL, TRUE, FALSE)          AS has_customer_account,
      customers_with_trial.first_customer_created_at,
      customers_with_trial.first_customer_id,
      customers_with_trial.customer_id_list,
      customers_with_trial.has_started_trial,
      customers_with_trial.has_started_trial_at

    FROM users
      LEFT JOIN highest_paid_subscription_plan
        ON users.user_id = highest_paid_subscription_plan.user_id
      LEFT JOIN customers_with_trial
        ON users.user_id::VARCHAR = customers_with_trial.user_id::VARCHAR
    WHERE {{ filter_out_active_users('users', 'user_id') }}

)

SELECT *
FROM joined

