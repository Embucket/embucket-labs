WITH all_milestones AS (

  SELECT

    id::NUMBER            AS milestone_id,
    title::VARCHAR        AS milestone_title,
    description::VARCHAR  AS milestone_description,
    project_id::NUMBER    AS project_id,
    group_id::NUMBER      AS group_id,
    start_date::DATE      AS start_date,
    due_date::DATE        AS due_date,
    state::VARCHAR        AS milestone_status,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at

  FROM {{ ref('gitlab_dotcom_milestones_dedupe_source') }}

),

internal_milestones AS (
  SELECT
    id::NUMBER            AS internal_milestone_id,
    iid::NUMBER           AS internal_milestone_iid,
    title::VARCHAR        AS internal_milestone_title,
    project_id::NUMBER    AS internal_project_id,
    updated_at::TIMESTAMP AS internal_updated_id
  FROM {{ ref('gitlab_dotcom_milestones_internal_only_dedupe_source') }}
),

combined AS (

  SELECT

    all_milestones.milestone_id                  AS milestone_id,
    internal_milestones.internal_milestone_title AS milestone_title,
    all_milestones.milestone_description         AS milestone_description,
    all_milestones.project_id                    AS project_id,
    all_milestones.group_id                      AS group_id,
    all_milestones.start_date                    AS start_date,
    all_milestones.due_date                      AS due_date,
    all_milestones.milestone_status              AS milestone_status,
    all_milestones.created_at                    AS created_at,
    all_milestones.updated_at                    AS updated_at

  FROM all_milestones
  LEFT JOIN internal_milestones
    ON all_milestones.milestone_id = internal_milestones.internal_milestone_id

)

SELECT *
FROM combined
