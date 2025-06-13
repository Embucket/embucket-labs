{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_resource_weight_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('prep_project', 'prep_project'),
    ('prep_issue', 'prep_issue')
]) }}

, resource_weight_events AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_weight_events_source') }}
    {% if is_incremental() %}

    WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      resource_weight_event_id                                      AS dim_resource_weight_id,
      resource_weight_events.user_id                                AS dim_user_id,
      resource_weight_events.created_at,
      dim_date.date_id                                              AS created_date_id,
      IFNULL(prep_project.dim_project_id, -1)                       AS dim_project_id,
      IFNULL(prep_project.ultimate_parent_namespace_id, -1)         AS ultimate_parent_namespace_id,
      IFNULL(prep_namespace_plan_hist.dim_plan_id, 34)              AS dim_plan_id
    FROM resource_weight_events
    LEFT JOIN prep_issue
      ON resource_weight_events.issue_id = prep_issue.issue_id
    LEFT JOIN prep_project
      ON prep_issue.dim_project_sk = prep_project.dim_project_sk
    LEFT JOIN prep_namespace_plan_hist
      ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
        AND resource_weight_events.created_at >= prep_namespace_plan_hist.valid_from
        AND resource_weight_events.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(resource_weight_events.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-04-01",
    updated_date="2023-09-29"
) }}
