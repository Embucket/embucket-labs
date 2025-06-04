{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_resource_milestone_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request'),
    ('prep_project', 'prep_project')
]) }}

, resource_milestone_events AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_milestone_events_source') }}
    {% if is_incremental() %}

    WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

) , joined AS (

    SELECT
      resource_milestone_events.resource_milestone_event_id                                   AS dim_resource_milestone_id,
      COALESCE(issue_project.project_id,
                merge_request_project.project_id)                                             AS dim_project_id,
      COALESCE(prep_issue.dim_plan_id_at_creation,
                prep_merge_request.dim_plan_id_at_creation)                                   AS dim_plan_id,
      COALESCE(prep_issue.ultimate_parent_namespace_id,
                prep_merge_request.ultimate_parent_namespace_id)                              AS ultimate_parent_namespace_id,
      user_id                                                                                 AS dim_user_id,
      prep_issue.dim_issue_sk,
      prep_merge_request.dim_milestone_sk,
      resource_milestone_events.created_at,
      dim_date.date_id                                                                        AS created_date_id
    FROM resource_milestone_events
    LEFT JOIN prep_issue
      ON resource_milestone_events.issue_id = prep_issue.issue_id
    LEFT JOIN prep_merge_request
      ON resource_milestone_events.merge_request_id = prep_merge_request.merge_request_id
    LEFT JOIN dim_date
      ON TO_DATE(resource_milestone_events.created_at) = dim_date.date_day
    LEFT JOIN prep_project issue_project
      ON prep_issue.dim_project_sk = issue_project.dim_project_sk
    LEFT JOIN prep_project merge_request_project
      ON prep_merge_request.dim_project_sk = merge_request_project.dim_project_sk


)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-03-23",
    updated_date="2023-10-30"
) }}
