{{ config({
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    "schema": "legacy",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

-- depends_on: {{ ref('internal_gitlab_namespaces') }}

WITH namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
    WHERE is_currently_valid = TRUE

), plans AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_plans')}}

),

extracted AS (
  SELECT
    namespace_id,
    parent_id,
    lineage AS upstream_lineage,
    ultimate_parent_id
  FROM namespaces
),

with_plans AS (

  SELECT
    extracted.*,
    COALESCE((ultimate_parent_id IN {{ get_internal_parent_namespaces() }}), FALSE)   AS namespace_is_internal,
    namespace_plans.plan_id                                                           AS namespace_plan_id,
    namespace_plans.plan_title                                                        AS namespace_plan_title,
    namespace_plans.plan_is_paid                                                      AS namespace_plan_is_paid,
    COALESCE(ultimate_parent_plans.plan_id, 34)                                       AS ultimate_parent_plan_id,
    CASE
    WHEN ultimate_parent_gitlab_subscriptions.is_trial AND COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) <> 34
      THEN 'Trial: Ultimate'
      ELSE COALESCE(ultimate_parent_plans.plan_title, 'Free')
    END                                                                               AS ultimate_parent_plan_title,
    CASE
      WHEN ultimate_parent_gitlab_subscriptions.is_trial AND COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) != 34
        THEN 'ultimate_trial'
      ELSE COALESCE(ultimate_parent_plans.plan_name, 'free')
    END                                                                               AS ultimate_parent_plan_name,
    CASE
    WHEN ultimate_parent_gitlab_subscriptions.is_trial
      THEN FALSE
      ELSE COALESCE(ultimate_parent_plans.plan_is_paid, FALSE)
    END                                                                               AS ultimate_parent_plan_is_paid,
    namespace_gitlab_subscriptions.max_seats_used                                     AS max_seats_used,
    namespace_gitlab_subscriptions.seats_in_use                                       AS seats_in_use,
    namespace_gitlab_subscriptions.seats                                              AS seats


  FROM extracted
    -- Get plan information for the namespace.
    LEFT JOIN gitlab_subscriptions AS namespace_gitlab_subscriptions
      ON extracted.namespace_id = namespace_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS namespace_plans
      ON COALESCE(namespace_gitlab_subscriptions.plan_id, 34) = namespace_plans.plan_id
    -- Get plan information for the ultimate parent namespace.
    LEFT JOIN gitlab_subscriptions AS ultimate_parent_gitlab_subscriptions
      ON extracted.ultimate_parent_id = ultimate_parent_gitlab_subscriptions.namespace_id
    LEFT JOIN plans AS ultimate_parent_plans
      ON COALESCE(ultimate_parent_gitlab_subscriptions.plan_id, 34) = ultimate_parent_plans.plan_id

)

SELECT *
FROM with_plans
