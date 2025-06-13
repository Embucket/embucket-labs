{{ config({
    "materialized": "ephemeral",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


WITH memberships AS (

    SELECT
      *,
      DECODE(membership_source_type,
          'individual_namespace', 0,
          'group_membership', 1,
          'project_membership', 2,
          'group_group_link', 3,
          'group_group_link_ancestor', 4,
          'project_group_link', 5,
          'project_group_link_ancestor', 6
      ) AS membership_source_type_order,
      IFF(namespace_id = ultimate_parent_id, TRUE, FALSE) AS is_ultimate_parent
    FROM {{ ref('gitlab_dotcom_memberships') }}
    WHERE ultimate_parent_plan_id != 34

), plans AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

), highest_paid_subscription_plan AS (

  SELECT DISTINCT

    user_id,

    COALESCE(
      MAX(plans.plan_is_paid) OVER (
        PARTITION BY user_id
      ),
    FALSE)   AS highest_paid_subscription_plan_is_paid,

    COALESCE(
      FIRST_VALUE(ultimate_parent_plan_id) OVER (
        PARTITION BY user_id
        ORDER BY ultimate_parent_plan_id DESC
        )
      , 34) AS highest_paid_subscription_plan_id,

    FIRST_VALUE(namespace_id) OVER (
      PARTITION BY user_id
      ORDER BY
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )       AS highest_paid_subscription_namespace_id,

    FIRST_VALUE(ultimate_parent_id) OVER (
      PARTITION BY user_id
      ORDER BY
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )       AS highest_paid_subscription_ultimate_parent_id,

    FIRST_VALUE(membership_source_type) OVER (
      PARTITION BY user_id
      ORDER BY
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )       AS highest_paid_subscription_inheritance_source_type,

    FIRST_VALUE(membership_source_id) OVER (
      PARTITION BY user_id
      ORDER BY
        ultimate_parent_plan_id DESC,
        membership_source_type_order,
        is_ultimate_parent DESC,
        membership_source_type
    )       AS highest_paid_subscription_inheritance_source_id

  FROM memberships
    LEFT JOIN plans
      ON memberships.ultimate_parent_plan_id = plans.plan_id

)

SELECT *
FROM highest_paid_subscription_plan
