-- say_do snippet 
-- Create table of milestone from and to dates from gitlab_dotcom_notes_xf table
WITH
milestones_cte AS (

  SELECT
    milestone_title AS milestone,
    start_date      AS iteration_start_date,
    due_date        AS iteration_end_date
  FROM {{ ref('gitlab_dotcom_milestones_xf') }} AS miles
  WHERE miles.namespace_id = 9970 AND REGEXP_LIKE(milestone_title, '\\d+\.\\d+')
    AND DATE_TRUNC('month', start_date) >= DATE_TRUNC('month', DATEADD('months', -24, CURRENT_DATE))
    AND start_date <= CURRENT_DATE

),

milestone_history_notes AS (
  SELECT DISTINCT
    noteable_id                                                                                   AS issue_id,
    notes.note,
    notes.created_at::DATE                                                                        AS valid_from,
    LAG(notes.created_at::DATE, 1) OVER (PARTITION BY noteable_id ORDER BY notes.created_at DESC) AS valid_to
  FROM {{ ref('gitlab_dotcom_notes_xf') }} AS notes
  INNER JOIN {{ ref ('dim_issue') }} AS issues ON notes.noteable_id = issues.issue_id
  LEFT JOIN milestones_cte AS x
  WHERE noteable_type = 'Issue'
    AND (notes.note LIKE '%changed milestone to%') --only concerned with milestone changes
    AND (issues.issue_closed_at >= x.iteration_start_date OR issues.issue_closed_at IS NULL) --issue was considered open as of beginning of iteration (aka it was closed on/after start of iteration or is currently open)
    AND issues.is_part_of_product = TRUE
  ORDER BY 3 DESC, 4 DESC
),

-- Create table of milestone from and to dates from gitlab_dotcom_resource_milestone_events table
milestone_history_events AS (
  SELECT DISTINCT
    added.issue_id,
    CONCAT('changed milestone to %"', milestones.milestone_title, '"')                          AS note,
    added.created_at                                                                            AS valid_from,
    CASE WHEN valid_from > removed.created_at::DATE THEN NULL ELSE removed.created_at::DATE END AS valid_to
  FROM {{ ref('gitlab_dotcom_resource_milestone_events') }} AS added
  INNER JOIN {{ ref('gitlab_dotcom_milestones_xf') }} AS milestones ON added.milestone_id = milestones.milestone_id
  INNER JOIN {{ ref('dim_issue') }} AS issues ON added.issue_id = issues.issue_id
  LEFT JOIN {{ ref('gitlab_dotcom_resource_milestone_events') }} AS removed ON added.issue_id = removed.issue_id AND removed.action_type = 'removed' AND added.milestone_id = removed.milestone_id
  LEFT JOIN milestones_cte AS x
  WHERE added.action_type = 'added'
    AND added.issue_id IS NOT NULL
    AND (issues.issue_closed_at >= x.iteration_start_date OR issues.issue_closed_at IS NULL) --issue was considered open as of beginning of iteration (aka it was closed on/after start of iteration or is currently open)
    AND issues.is_part_of_product = TRUE
),

milestone_history_a AS (
  SELECT * FROM milestone_history_notes
  WHERE (valid_to != valid_from OR valid_to IS NULL)
  UNION ALL
  SELECT * FROM milestone_history_events
),

-- Lag function
milestone_history_b AS (
  SELECT
    milestone_history_a.*,
    LAG(DATEADD('day', 0, valid_from), 1) OVER (PARTITION BY issue_id ORDER BY valid_from DESC) AS valid_to_b
  FROM milestone_history_a
),

milestone_history AS (
  SELECT
    issue_id,
    note,
    valid_from,
    COALESCE(valid_to, valid_to_b) AS valid_to
  FROM milestone_history_b
),

-- Only issues that were in milestone during iteration
milestone_valid_dates AS (
  SELECT milestone_history.*
  FROM milestone_history
  LEFT JOIN milestones_cte AS x
  WHERE
    (valid_from BETWEEN x.iteration_start_date AND x.iteration_end_date)
    OR (valid_to BETWEEN x.iteration_start_date AND x.iteration_end_date)
    OR (valid_from <= x.iteration_start_date AND (valid_to IS NULL OR valid_to BETWEEN CURRENT_DATE AND x.iteration_end_date))
),

-- Only issues that had a ~Deliverable label during iteration
label_valid_dates AS (
  SELECT
    links.target_id                   AS issue_id,
    labels_xf.masked_label_title      AS note,
    links.label_link_created_at::DATE AS valid_from,
    links.valid_to::DATE              AS valid_to
  FROM {{ ref('gitlab_dotcom_label_links') }} AS links
  INNER JOIN {{ ref('gitlab_dotcom_labels_xf') }} AS labels_xf ON links.label_id = labels_xf.label_id
  INNER JOIN {{ ref('dim_issue') }} AS issues ON links.target_id = issues.issue_id
  LEFT JOIN milestones_cte AS x
  WHERE target_type = 'Issue'
    AND labels_xf.masked_label_title = 'Deliverable'
    AND
    (
      (links.label_link_created_at::DATE BETWEEN x.iteration_start_date AND x.iteration_end_date)
      OR (links.valid_to::DATE BETWEEN x.iteration_start_date AND x.iteration_end_date)
      OR (links.label_link_created_at::DATE <= x.iteration_start_date AND (links.valid_to IS NULL OR links.valid_to::DATE BETWEEN CURRENT_DATE AND x.iteration_end_date))
    )
    AND (issues.issue_closed_at >= x.iteration_start_date OR issues.issue_closed_at IS NULL) --issue was considered open as of beginning of iteration (aka it was closed on/after start of iteration or is currently open)
    AND issues.is_part_of_product = TRUE
),

-- Issues that were not closed before the iteration ended but had ~workflow% labels before iteration ended
issues_in_workflow AS (
  SELECT
    links.target_id                   AS issue_id,
    labels_xf.masked_label_title      AS note,
    links.label_link_created_at::DATE AS valid_from,
    links.valid_to::DATE              AS valid_to
  FROM {{ ref('gitlab_dotcom_label_links') }} AS links
  INNER JOIN {{ ref('gitlab_dotcom_labels_xf') }} AS labels_xf ON links.label_id = labels_xf.label_id
  INNER JOIN {{ ref('dim_issue') }} AS issues ON links.target_id = issues.issue_id
  LEFT JOIN milestones_cte AS x
  WHERE target_type = 'Issue'
    AND (labels_xf.masked_label_title LIKE '%workflow::production%' OR labels_xf.masked_label_title LIKE '%workflow::verification%')
    AND
    (
      (links.label_link_created_at::DATE BETWEEN x.iteration_start_date AND x.iteration_end_date)
      OR (links.valid_to::DATE BETWEEN x.iteration_start_date AND x.iteration_end_date)
      OR (links.label_link_created_at::DATE <= x.iteration_start_date AND (links.valid_to IS NULL OR links.valid_to::DATE BETWEEN CURRENT_DATE AND x.iteration_end_date))
    )
    AND (issues.issue_closed_at >= x.iteration_start_date OR issues.issue_closed_at IS NULL) --issue was considered open as of beginning of iteration (aka it was closed on/after start of iteration or is currently open)
    AND issues.is_part_of_product = TRUE
),

-- Compile issues with correct milestone or ~Deliverable label during iteration
aggregated_milestone_label AS (
  SELECT *
  FROM milestone_valid_dates
  UNION ALL
  SELECT *
  FROM label_valid_dates
),

committed_issues_v1 AS (
  SELECT DISTINCT
    a.*,
    b.valid_from                                                                                                                AS valid_from2,
    b.valid_to                                                                                                                  AS valid_to2,
    CASE WHEN DATE(issues.issue_closed_at) <= x.iteration_end_date OR issues_in_workflow.issue_id IS NOT NULL THEN 1 ELSE 0 END AS is_closed_before_release,
    labels,
    issues.dim_project_sk                                                                                                       AS project_id,
    issues.issue_title,
    issues.issue_internal_id                                                                                                    AS issue_iid,
    issues.created_at::DATE                                                                                                     AS issue_created_at,
    issues.issue_closed_at::DATE                                                                                                AS issue_closed_at,
    issues.weight,
    CASE WHEN a.valid_to <= x.iteration_end_date OR b.valid_to <= x.iteration_end_date THEN 1 ELSE 0 END                        AS removed
  FROM aggregated_milestone_label AS a
  INNER JOIN {{ ref('dim_issue') }} AS issues ON a.issue_id = issues.issue_id
  INNER JOIN aggregated_milestone_label AS b ON b.note LIKE '%Deliverable%' AND a.issue_id = b.issue_id AND b.valid_from BETWEEN a.valid_from AND COALESCE(a.valid_to, CURRENT_DATE)
  LEFT JOIN issues_in_workflow ON issues.issue_id = issues_in_workflow.issue_id
  LEFT JOIN milestones_cte AS x
  UNION ALL
  SELECT DISTINCT
    a.*,
    b.valid_from                                                                                                                AS valid_from2,
    b.valid_to                                                                                                                  AS valid_to2,
    CASE WHEN DATE(issues.issue_closed_at) <= x.iteration_end_date OR issues_in_workflow.issue_id IS NOT NULL THEN 1 ELSE 0 END AS is_closed_before_release,
    labels,
    issues.dim_project_sk                                                                                                       AS project_id,
    issues.issue_title,
    issues.issue_internal_id                                                                                                    AS issue_iid,
    issues.created_at::DATE                                                                                                     AS issue_created_at,
    issues.issue_closed_at::DATE                                                                                                AS issue_closed_at,
    issues.weight,
    CASE WHEN a.valid_to <= x.iteration_end_date OR b.valid_to <= x.iteration_end_date THEN 1 ELSE 0 END                        AS removed
  FROM aggregated_milestone_label AS a
  INNER JOIN {{ ref('dim_issue') }} AS issues ON a.issue_id = issues.issue_id
  INNER JOIN aggregated_milestone_label AS b ON b.note LIKE '%[milestone]%' AND a.issue_id = b.issue_id AND b.valid_from BETWEEN a.valid_from AND COALESCE(a.valid_to, CURRENT_DATE)
  LEFT JOIN issues_in_workflow ON issues.issue_id = issues_in_workflow.issue_id
  LEFT JOIN milestones_cte AS x
  WHERE a.note LIKE '%Deliverable%'
),

-- Append issue URL
committed_issues_v2 AS (
  SELECT
    committed_issues_v1.*,
    CASE
      WHEN grps_p4.group_id IS NOT NULL THEN grps_p4.group_path || '/' || grps_p3.group_path || '/' || grps_p2.group_path || '/' || grps_p1.group_path || '/' || grps.group_path
      WHEN grps_p3.group_id IS NOT NULL THEN grps_p3.group_path || '/' || grps_p2.group_path || '/' || grps_p1.group_path || '/' || grps.group_path
      WHEN grps_p2.group_id IS NOT NULL THEN grps_p2.group_path || '/' || grps_p1.group_path || '/' || grps.group_path
      WHEN grps_p1.group_id IS NOT NULL THEN grps_p1.group_path || '/' || grps.group_path
      ELSE grps.group_path
    END                                                                                                                                                                AS full_group_path,
    '[' || REPLACE(REPLACE(issue_title, '[', ''), ']', '') || '](https://gitlab.com/' || full_group_path || '/' || proj.project_path || '/issues/' || issue_iid || ')' AS linked_issue_title
  FROM committed_issues_v1
  INNER JOIN {{ ref('gitlab_dotcom_projects_xf') }} AS proj ON committed_issues_v1.project_id = proj.project_id
  LEFT OUTER JOIN {{ ref('gitlab_dotcom_groups_xf') }} AS grps ON proj.namespace_id = grps.group_id
  LEFT OUTER JOIN {{ ref('gitlab_dotcom_groups_xf') }} AS grps_p1 ON grps.parent_group_id = grps_p1.group_id AND grps.is_top_level_group = FALSE
  LEFT OUTER JOIN {{ ref('gitlab_dotcom_groups_xf') }} AS grps_p2 ON grps_p1.parent_group_id = grps_p2.group_id AND grps_p1.is_top_level_group = FALSE
  LEFT OUTER JOIN {{ ref('gitlab_dotcom_groups_xf') }} AS grps_p3 ON grps_p2.parent_group_id = grps_p3.group_id AND grps_p2.is_top_level_group = FALSE
  LEFT OUTER JOIN {{ ref('gitlab_dotcom_groups_xf') }} AS grps_p4 ON grps_p3.parent_group_id = grps_p4.group_id AND grps_p3.is_top_level_group = FALSE
),

-- Add stage labels
stage_labels AS (
  SELECT DISTINCT
    committed_issues_v2.issue_id,
    SUBSTRING(REPLACE(value, '"', ''), 9) AS stage
  FROM committed_issues_v2, LATERAL FLATTEN(input => labels)
  WHERE value LIKE 'devops::%'
    AND value NOT LIKE '%::%::%'
),

-- Add group labels
group_labels AS (
  SELECT DISTINCT
    committed_issues_v2.issue_id,
    SUBSTRING(REPLACE(value, '"', ''), 8) AS group_
  FROM committed_issues_v2, LATERAL FLATTEN(input => labels)
  WHERE value LIKE 'group::%'
),

milestones_info AS (
  SELECT
    mh.issue_id,
    mc.milestone
  FROM milestone_history mh
  JOIN milestones_cte mc
    ON mh.valid_from BETWEEN mc.iteration_start_date AND mc.iteration_end_date
    OR (mh.valid_to IS NOT NULL AND mh.valid_to BETWEEN mc.iteration_start_date AND mc.iteration_end_date)
)


SELECT DISTINCT
  ci.issue_id,
  ci.issue_created_at,
  ci.issue_closed_at,
  ci.linked_issue_title,
  ci.is_closed_before_release,
  ci.labels,
  ci.weight,
  TRIM(stage_labels.stage)         AS stage,
  TRIM(group_labels.group_)        AS group_,
  MAX(ci.removed) AS removed,
  milestones_info.milestone         AS milestone
FROM committed_issues_v2 ci
LEFT JOIN stage_labels ON ci.issue_id = stage_labels.issue_id
LEFT JOIN group_labels ON ci.issue_id = group_labels.issue_id
LEFT JOIN milestones_info ON ci.issue_id = milestones_info.issue_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, milestone
