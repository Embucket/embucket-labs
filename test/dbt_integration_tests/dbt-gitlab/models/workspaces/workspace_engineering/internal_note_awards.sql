WITH internal_projects AS (

    SELECT
      ultimate_parent_namespace_id,
      dim_namespace_id              AS namespace_id,
      project_id,
      dim_project_sk
    FROM {{ ref('dim_project') }}
    WHERE namespace_is_internal = TRUE

), merge_requests AS (

    SELECT
      dim_merge_request.merge_request_id              AS merge_request_id,
      dim_project.project_id                          AS project_id,
      internal_projects.ultimate_parent_namespace_id,
      internal_projects.namespace_id
    FROM {{ ref('dim_merge_request') }}
    LEFT JOIN {{ ref('dim_project') }}
      ON dim_merge_request.dim_project_sk = dim_project.dim_project_sk
    INNER JOIN internal_projects
      ON internal_projects.project_id = dim_project.project_id

), issues AS (

    SELECT
      dim_issue.issue_id                              AS issue_id,
      internal_projects.project_id                    AS project_id,
      internal_projects.ultimate_parent_namespace_id,
      internal_projects.namespace_id
    FROM {{ ref('dim_issue') }}
    INNER JOIN internal_projects
      ON internal_projects.dim_project_sk = dim_issue.dim_project_sk

), notes AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_source') }}
    WHERE system = FALSE

), awards AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_award_emoji_source') }}

), internal_notes AS (

    SELECT
      COALESCE(merge_requests.ultimate_parent_namespace_id,
               issues.ultimate_parent_namespace_id)              AS ultimate_parent_namespace_id,
      COALESCE(merge_requests.namespace_id, issues.namespace_id) AS namespace_id,
      COALESCE(merge_requests.project_id, issues.project_id)     AS project_id,
      noteable_type,
      merge_request_id,
      issue_id,
      note_id,
      note_author_id
    FROM notes
    LEFT JOIN merge_requests
      ON merge_requests.merge_request_id = notes.noteable_id
      AND notes.noteable_type = 'MergeRequest'
    LEFT JOIN issues
      ON issues.issue_id = notes.noteable_id
      AND notes.noteable_type = 'Issue'
    WHERE (merge_requests.merge_request_id IS NOT NULL
      OR issues.issue_id IS NOT NULL)

), internal_note_awards AS (
    SELECT
      internal_notes.*,
      awards.award_emoji_id,
      awards.award_emoji_name,
      awards.user_id           AS awarder_user_id
    FROM internal_notes
    LEFT JOIN awards
      ON internal_notes.note_id = awards.awardable_id
      AND awards.awardable_type = 'Note'

)

SELECT *
FROM internal_note_awards