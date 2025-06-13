    
WITH all_issues AS (

  SELECT  
    id::NUMBER                                               AS issue_id,
    iid::NUMBER                                              AS issue_iid,
    author_id::NUMBER                                        AS author_id,
    project_id::NUMBER                                       AS project_id,
    milestone_id::NUMBER                                     AS milestone_id,
    sprint_id::NUMBER                                        AS sprint_id,
    updated_by_id::NUMBER                                    AS updated_by_id,
    last_edited_by_id::NUMBER                                AS last_edited_by_id,
    moved_to_id::NUMBER                                      AS moved_to_id,
    created_at::TIMESTAMP                                    AS created_at,
    updated_at::TIMESTAMP                                    AS updated_at,
    last_edited_at::TIMESTAMP                                AS issue_last_edited_at,
    closed_at::TIMESTAMP                                     AS issue_closed_at,
    confidential::BOOLEAN                                    AS is_confidential,
    pgp_is_deleted::BOOLEAN                                  AS is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP                     AS is_deleted_updated_at,
    title::VARCHAR                                           AS issue_title,
    description::VARCHAR                                     AS issue_description,

    -- Override state by mapping state_id. See issue #3344.
    {{ map_state_id('state_id') }}                           AS state,

    weight::NUMBER                                           AS weight,
    due_date::DATE                                           AS due_date,
    lock_version::NUMBER                                     AS lock_version,
    time_estimate::NUMBER                                    AS time_estimate,
    discussion_locked::BOOLEAN                               AS has_discussion_locked,
    closed_by_id::NUMBER                                     AS closed_by_id,
    relative_position::NUMBER                                AS relative_position,
    service_desk_reply_to::VARCHAR                           AS service_desk_reply_to,
    state_id::NUMBER                                         AS state_id,
    duplicated_to_id::NUMBER                                 AS duplicated_to_id,
    promoted_to_epic_id::NUMBER                              AS promoted_to_epic_id,
    work_item_type_id                                        AS work_item_type_id
  FROM {{ ref('gitlab_dotcom_issues_dedupe_source') }}
  WHERE created_at::VARCHAR NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
    AND LEFT(created_at::VARCHAR , 10) != '1970-01-01'
  
), 

internal_only AS (

  SELECT
    id::NUMBER                                               AS internal_issue_id,
    iid::NUMBER                                              AS internal_issue_iid,
    title::VARCHAR                                           AS internal_issue_title,
    description::VARCHAR                                     AS internal_issue_description,
    project_id::NUMBER                                       AS internal_project_id,
    service_desk_reply_to::VARCHAR                           AS internal_service_desk_reply_to
  FROM {{ ref('gitlab_dotcom_issues_internal_only_dedupe_source') }}
  
),

combined AS (

  SELECT
    all_issues.issue_id                                      AS issue_id,
    all_issues.issue_iid                                     AS issue_iid,
    all_issues.author_id                                     AS author_id,
    all_issues.project_id                                    AS project_id,
    all_issues.milestone_id                                  AS milestone_id,
    all_issues.sprint_id                                     AS sprint_id,
    all_issues.updated_by_id                                 AS updated_by_id,
    all_issues.last_edited_by_id                             AS last_edited_by_id,
    all_issues.moved_to_id                                   AS moved_to_id,
    all_issues.created_at                                    AS created_at,
    all_issues.updated_at                                    AS updated_at,
    all_issues.issue_last_edited_at                          AS issue_last_edited_at,
    all_issues.issue_closed_at                               AS issue_closed_at,
    all_issues.is_confidential                               AS is_confidential,
    all_issues.is_deleted                                    AS is_deleted,
    all_issues.is_deleted_updated_at                         AS is_deleted_updated_at,
    internal_only.internal_issue_title                       AS issue_title,
    internal_only.internal_issue_description                 AS issue_description,
    all_issues.state                                         AS state,
    all_issues.weight                                        AS weight,
    all_issues.due_date                                      AS due_date,
    all_issues.lock_version                                  AS lock_version,
    all_issues.time_estimate                                 AS time_estimate,
    all_issues.has_discussion_locked                         AS has_discussion_locked,
    all_issues.closed_by_id                                  AS closed_by_id,
    all_issues.relative_position                             AS relative_position,
    internal_only.internal_service_desk_reply_to             AS service_desk_reply_to,
    all_issues.state_id                                      AS state_id,
    all_issues.duplicated_to_id                              AS duplicated_to_id,
    all_issues.promoted_to_epic_id                           AS promoted_to_epic_id,
    all_issues.work_item_type_id                             AS work_item_type_id
  FROM all_issues
  LEFT JOIN internal_only 
    ON internal_only.internal_issue_id = all_issues.issue_id
  
)

SELECT *
FROM combined
