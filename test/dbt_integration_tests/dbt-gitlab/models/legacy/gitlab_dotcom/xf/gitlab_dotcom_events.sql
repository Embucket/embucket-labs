{{ config({
    "materialized": "incremental",
    "unique_key": "event_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_events_source') }}
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}

), plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

), users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }} users
    WHERE {{ filter_out_blocked_users('users', 'user_id') }}

), joined AS (

    SELECT
      source.*,
      projects.ultimate_parent_id,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END                                 AS plan_id_at_event_date,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(plans.plan_name, 'free')
      END                                 AS plan_name_at_event_date,
      COALESCE(plans.plan_is_paid, FALSE) AS plan_was_paid_at_event_date,
      users.created_at                    AS user_created_at
    FROM source
      LEFT JOIN projects
        ON source.project_id = projects.project_id
      LEFT JOIN gitlab_subscriptions
        ON projects.ultimate_parent_id = gitlab_subscriptions.namespace_id
        AND source.created_at BETWEEN gitlab_subscriptions.valid_from
        AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
      LEFT JOIN users
        ON source.author_id = users.user_id

)

SELECT *
FROM joined
ORDER BY updated_at
