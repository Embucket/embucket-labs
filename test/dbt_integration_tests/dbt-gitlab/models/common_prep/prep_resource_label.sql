{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_issue_label_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_epic', 'prep_epic'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request'),
    ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
    ('prep_project', 'prep_project')
]) }}

, resource_label_events AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_label_events_source') }}
    {% if is_incremental() %}

    WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

), namespace_prep AS (

   SELECT *
   FROM {{ ref('prep_namespace') }}
   WHERE is_currently_valid = TRUE

) , joined AS (

    SELECT
      resource_label_events.resource_label_event_id                         AS dim_issue_label_id,
      COALESCE(issue_project.project_id,
                mr_project.project_id)                                      AS dim_project_id,
      COALESCE(prep_epic.dim_plan_id_at_creation,
                prep_issue.dim_plan_id_at_creation,
                prep_merge_request.dim_plan_id_at_creation)                 AS dim_plan_id,
      COALESCE(namespace_prep.namespace_id,
                prep_issue.ultimate_parent_namespace_id,
                prep_merge_request.ultimate_parent_namespace_id)            AS ultimate_parent_namespace_id,
      user_id                                                               AS dim_user_id,
      prep_issue.dim_issue_sk                                               AS dim_issue_sk,
      prep_merge_request.dim_merge_request_sk                               AS dim_merge_request_sk,
      prep_epic.dim_epic_sk                                                 AS dim_epic_sk,
      resource_label_events.created_at::TIMESTAMP                           AS created_at,
      dim_date.date_id                                                      AS created_date_id
    FROM resource_label_events
    LEFT JOIN prep_epic
      ON resource_label_events.epic_id = prep_epic.epic_id
    LEFT JOIN prep_issue
      ON resource_label_events.issue_id = prep_issue.issue_id
    LEFT JOIN prep_merge_request
      ON resource_label_events.merge_request_id = prep_merge_request.merge_request_id
    INNER JOIN dim_date
      ON TO_DATE(resource_label_events.created_at) = dim_date.date_day
    LEFT JOIN namespace_prep
      ON prep_epic.dim_namespace_sk = namespace_prep.dim_namespace_sk
    LEFT JOIN prep_gitlab_dotcom_plan
      ON prep_epic.dim_plan_sk_at_creation = prep_gitlab_dotcom_plan.dim_plan_sk
    LEFT JOIN prep_project AS issue_project
      ON prep_issue.dim_project_sk = issue_project.dim_project_sk
    LEFT JOIN prep_project AS mr_project
      ON prep_merge_request.dim_project_sk = mr_project.dim_project_sk

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-03-14",
    updated_date="2023-10-30"
) }}
