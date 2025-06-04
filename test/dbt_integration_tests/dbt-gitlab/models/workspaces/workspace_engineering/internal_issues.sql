-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

WITH internal_projects AS (

  SELECT *
  FROM {{ ref('map_project_internal') }}

),

issues AS (

  SELECT
    issues.*,
    internal_projects.parent_namespace_id AS namespace_id,
    internal_projects.ultimate_parent_namespace_id,
    gitlab_dotcom_work_item_type_source.work_item_type_name AS issue_type
  FROM {{ ref('gitlab_dotcom_issues_source') }} AS issues
  INNER JOIN internal_projects
    ON issues.project_id = internal_projects.project_id
  LEFT JOIN {{ ref('gitlab_dotcom_work_item_type_source') }}
    ON issues.work_item_type_id = gitlab_dotcom_work_item_type_source.work_item_type_id


),

label_links AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_label_links') }}
  WHERE is_currently_valid = TRUE
    AND target_type = 'Issue'

),

all_labels AS (

  SELECT labels.*
  FROM {{ ref('gitlab_dotcom_labels_source') }} AS labels

),

close_moved_date AS (

  -- Derive close date from the latest note from an issue, when it contains closed or moved details

  SELECT
    noteable_id AS issue_id,
    created_at  AS derived_closed_at
  FROM {{ ref('gitlab_dotcom_notes_source') }}
  WHERE noteable_type = 'Issue'
    AND system = TRUE
    AND (
      CONTAINS(note, 'closed')
      OR CONTAINS(note, 'moved to')
      OR note ILIKE ANY ('Status changed to closed%', 'closed via%')
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY noteable_id ORDER BY created_at DESC) = 1

),

derived_close_date AS (

  -- Derive close date from the latest note from an issue, regardless of the type of note

  SELECT
    noteable_id AS issue_id,
    created_at  AS derived_closed_at
  FROM {{ ref('gitlab_dotcom_notes_source') }}
  WHERE noteable_type = 'Issue'
    AND system = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY noteable_id ORDER BY created_at DESC) = 1

),

agg_labels AS (

  SELECT
    issues.issue_id,
    ARRAY_AGG(LOWER(all_labels.label_title)) WITHIN GROUP (ORDER BY all_labels.label_title ASC) AS labels
  FROM issues
  LEFT JOIN label_links
    ON issues.issue_id = label_links.target_id
  LEFT JOIN all_labels
    ON label_links.label_id = all_labels.label_id
  GROUP BY 1

),

issue_metrics AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issue_metrics_source') }}

),

events_weight AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_resource_weight_events_source') }}

),

first_events_weight AS (

  SELECT
    issue_id,
    MIN(created_at) AS first_weight_set_at
  FROM events_weight
  GROUP BY 1

),

epic_to_issue AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_epic_issues_source') }}

),

prep_epic AS (

  SELECT *
  FROM {{ ref('prep_epic') }}

),

gitlab_user AS (

  SELECT *
  FROM {{ ref('dim_gitlab_dotcom_gitlab_emails') }}

),

issue_to_assignee AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issue_assignees_source') }}

),

list_assignees AS (

  SELECT
    a.issue_id,
    LISTAGG(gitlab_dotcom_user_name, ' | ') AS issue_assignee_user_name
  FROM issue_to_assignee AS a
  INNER JOIN gitlab_user AS b ON a.user_id = b.gitlab_dotcom_user_id
  GROUP BY 1

),

joined AS (

  SELECT
    issues.issue_id,
    issues.issue_iid,
    issues.author_id,
    issues.project_id,
    iff(issues.is_confidential, 'confidential - masked', issues.issue_title) as issue_title,
    iff(issues.is_confidential, 'confidential - masked', issues.issue_description) as issue_description,
    issues.namespace_id,
    issues.ultimate_parent_namespace_id                                                                                                                              AS ultimate_parent_id,
    issues.milestone_id,
    issues.sprint_id,
    issues.updated_by_id,
    issues.last_edited_by_id,
    issues.moved_to_id,
    issues.created_at                                                                                                                                                AS issue_created_at,
    issues.updated_at                                                                                                                                                AS issue_updated_at,
    issues.issue_last_edited_at,
    /*
    If the issue is closed, then get the closed_at date from one of the following sources:
    - The closed_at date from the raw data
    - The derived_closed_at date from the last moved or closed note in the notes_source
    - The derived_closed_at date from the last note left in the issue (regardless of the type of note)
    */
    IFF(issues.state = 'closed', COALESCE(issues.issue_closed_at, close_moved_date.derived_closed_at, derived_close_date.derived_closed_at), issues.issue_closed_at)
      AS issue_closed_at,
    issues.is_confidential                                                                                                                                           AS issue_is_confidential,
    COALESCE(
      issues.namespace_id = 9970
      AND ARRAY_CONTAINS('community contribution'::VARIANT, agg_labels.labels), FALSE
    )                                                                                                                                                                AS is_community_contributor_related,

    CASE
      WHEN ARRAY_CONTAINS('severity::1'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('S1'::VARIANT, agg_labels.labels)
        THEN 'severity 1'
      WHEN ARRAY_CONTAINS('severity::2'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('S2'::VARIANT, agg_labels.labels)
        THEN 'severity 2'
      WHEN ARRAY_CONTAINS('severity::3'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('S3'::VARIANT, agg_labels.labels)
        THEN 'severity 3'
      WHEN ARRAY_CONTAINS('severity::4'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('S4'::VARIANT, agg_labels.labels)
        THEN 'severity 4'
      ELSE 'undefined'
    END                                                                                                                                                              AS severity_tag,
    CASE
      WHEN ARRAY_CONTAINS('priority::1'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('P1'::VARIANT, agg_labels.labels)
        THEN 'priority 1'
      WHEN ARRAY_CONTAINS('priority::2'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('P2'::VARIANT, agg_labels.labels)
        THEN 'priority 2'
      WHEN ARRAY_CONTAINS('priority::3'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('P3'::VARIANT, agg_labels.labels)
        THEN 'priority 3'
      WHEN ARRAY_CONTAINS('priority::4'::VARIANT, agg_labels.labels) OR ARRAY_CONTAINS('P4'::VARIANT, agg_labels.labels)
        THEN 'priority 4'
      ELSE 'undefined'
    END                                                                                                                                                              AS priority_tag,
    COALESCE(
      issues.namespace_id = 9970
      AND ARRAY_CONTAINS('security'::VARIANT, agg_labels.labels), FALSE
    )                                                                                                                                                                AS is_security_issue,
    IFF(
      issues.project_id IN ({{ is_project_included_in_engineering_metrics() }}),
      TRUE, FALSE
    )                                                                                                                                                                AS is_included_in_engineering_metrics,
    IFF(
      issues.project_id IN ({{ is_project_part_of_product() }}),
      TRUE, FALSE
    )                                                                                                                                                                AS is_part_of_product,
    issues.state,
    issues.weight,
    issues.due_date,
    issues.lock_version,
    issues.time_estimate,
    issues.has_discussion_locked,
    issues.closed_by_id,
    issues.relative_position,
    issues.service_desk_reply_to,
    issues.duplicated_to_id,
    issues.promoted_to_epic_id,
    issues.issue_type,
    agg_labels.labels,
    ARRAY_TO_STRING(agg_labels.labels, '|')                                                                                                                          AS masked_label_title,
    issue_metrics.first_mentioned_in_commit_at,
    issue_metrics.first_associated_with_milestone_at,
    issue_metrics.first_added_to_board_at,
    first_events_weight.first_weight_set_at,
    prep_epic.epic_id,
    prep_epic.epic_title,
    prep_epic.labels                                                                                                                                                 AS epic_labels,
    prep_epic.epic_state,
    list_assignees.issue_assignee_user_name
  FROM issues
  LEFT JOIN agg_labels
    ON issues.issue_id = agg_labels.issue_id
  LEFT JOIN issue_metrics
    ON issues.issue_id = issue_metrics.issue_id
  LEFT JOIN first_events_weight
    ON issues.issue_id = first_events_weight.issue_id
  LEFT JOIN close_moved_date
    ON issues.issue_id = close_moved_date.issue_id
  LEFT JOIN derived_close_date
    ON issues.issue_id = derived_close_date.issue_id
  LEFT JOIN epic_to_issue
    ON issues.issue_id = epic_to_issue.issue_id
  LEFT JOIN prep_epic
    ON epic_to_issue.epic_id = prep_epic.epic_id
  LEFT JOIN list_assignees
    ON issues.issue_id = list_assignees.issue_id

)

SELECT *
FROM joined
