{{ simple_cte([('assigend_users','infradev_current_issue_users')]) }},

issues AS (
  SELECT *
  FROM {{ ref('internal_issues_enhanced') }}
),

label_groups AS (
  SELECT *
  FROM {{ ref('labels_history') }}
),

dates AS (

  SELECT
    *,
    MIN(date_actual) OVER () AS min_date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual > DATE_TRUNC('month', DATEADD('year', -2, CURRENT_DATE()))
    AND date_actual < CURRENT_DATE()
),

workflow_labels AS (

  SELECT * FROM {{ ref('engineering_analytics_workflow_labels') }}

),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['dates.date_actual','issues.issue_id']) }} AS daily_issue_id,
    dates.date_actual,
    issues.issue_id,
    issues.issue_iid,
    issues.project_id,
    issues.project_path,
    issues.namespace_id,
    issues.ultimate_parent_id,
    issues.labels,
    issues.masked_label_title,
    issues.issue_title,
    issues.full_group_path,
    issues.url,
    issues.milestone_id,
    issues.milestone_title,
    issues.milestone_description,
    issues.milestone_start_date,
    issues.milestone_due_date,
    issues.milestone_recency,
    issues.author_id,
    issues.is_created_by_bot,
    issues.weight,

    IFF(dates.date_actual >= DATE_TRUNC('day', issues.closed_at), 'closed', 'opened')                                                                                                  AS issue_state,
    issues.created_at                                                                                                                                                                  AS issue_created_at,
    issues.closed_at                                                                                                                                                                   AS issue_closed_at,
    issues.priority_label,
    COALESCE(severity.severity, 'No Severity')                                                                                                                                         AS severity,
    severity.label_added_at                                                                                                                                                            AS severity_label_added_at,
    severity.label_valid_to                                                                                                                                                            AS severity_label_valid_to,
    COALESCE(team.assigned_team, 'Unassigned')                                                                                                                                         AS assigned_team,
    team.label_added_at                                                                                                                                                                AS team_label_added_at,
    team.label_valid_to                                                                                                                                                                AS team_label_valid_to,
    team.label_title                                                                                                                                                                   AS team_label,
    wf.label_added_at                                                                                                                                                                  AS workflow_label_added_at,
    wf.label_valid_to                                                                                                                                                                  AS workflow_label_valid_to,
    wf.workflow                                                                                                                                                                        AS workflow_label,
    wfl.cycle,
    IFF(
      dates.date_actual > DATE_TRUNC('day', issues.closed_at), DATEDIFF('day', issues.created_at, issues.closed_at),
      DATEDIFF('day', issues.created_at, dates.date_actual)
    )                                                                                                                                                                                  AS issue_open_age_in_days,
    IFF(
      dates.date_actual > DATE_TRUNC('day', issues.closed_at), DATEDIFF('day', severity.label_added_at, issues.closed_at),
      DATEDIFF('day', severity.label_added_at, dates.date_actual)
    )                                                                                                                                                                                  AS severity_label_age_in_days,
    assigend_users.assigned_usernames,
    IFF(assigend_users.assigned_usernames IS NULL, TRUE, FALSE)                                                                                                                        AS is_issue_unassigned,
    issues.group_label,
    issues.section_label,
    issues.stage_label,
    issues.type_label,
    issues.subtype_label,
    issues.is_infradev,
    issues.fedramp_vulnerability,
    issues.is_community_contribution,
    issues.is_security,
    issues.is_corrective_action,
    issues.is_sus_impacting,
    issues.is_part_of_product,
    CASE WHEN issues.is_security
        THEN (CASE severity.severity
          WHEN 'S1' THEN 30
          WHEN 'S2' THEN 30
          WHEN 'S3' THEN 90
          WHEN 'S4' THEN 180
        END)
      ELSE (CASE severity.severity
        WHEN 'S1' THEN 30
        WHEN 'S2' THEN 60
        WHEN 'S3' THEN 90
        WHEN 'S4' THEN 120
      END)
    END                                                                                                                                                                                AS slo,
    /*
    Request to have 'risk treatment::operational requirement' exempt from past-due KPI metrics
    https://gitlab.com/gitlab-org/quality/engineering-analytics/team-tasks/-/issues/335
    */
    CASE WHEN issues.is_security THEN (CASE
        WHEN ARRAY_CONTAINS(CAST('risk treatment::operational requirement' AS VARIANT), issues.labels) THEN 0
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S1' THEN 1
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S2' THEN 1
        WHEN severity_label_age_in_days > 90
          AND severity.severity = 'S3' THEN 1
        WHEN severity_label_age_in_days > 180
          AND severity.severity = 'S4' THEN 1
        ELSE 0
      END) ELSE (CASE
        WHEN ARRAY_CONTAINS(CAST('risk treatment::operational requirement' AS VARIANT), issues.labels) THEN 0
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S1' THEN 1
        WHEN severity_label_age_in_days > 60
          AND severity.severity = 'S2' THEN 1
        WHEN severity_label_age_in_days > 90
          AND severity.severity = 'S3' THEN 1
        WHEN severity_label_age_in_days > 120
          AND severity.severity = 'S4' THEN 1
        ELSE 0
      END)
    END                                                                                                                                                                                AS slo_breach_counter,
    CASE WHEN issues.is_security THEN (CASE
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S1' THEN (severity_label_age_in_days - 30)
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S2' THEN (severity_label_age_in_days - 30)
        WHEN severity_label_age_in_days > 90
          AND severity.severity = 'S3' THEN (severity_label_age_in_days - 90)
        WHEN severity_label_age_in_days > 180
          AND severity.severity = 'S4' THEN (severity_label_age_in_days - 180)
        ELSE 0
      END) ELSE (CASE
        WHEN severity_label_age_in_days > 30
          AND severity.severity = 'S1' THEN (severity_label_age_in_days - 30)
        WHEN severity_label_age_in_days > 60
          AND severity.severity = 'S2' THEN (severity_label_age_in_days - 60)
        WHEN severity_label_age_in_days > 90
          AND severity.severity = 'S3' THEN (severity_label_age_in_days - 90)
        WHEN severity_label_age_in_days > 120
          AND severity.severity = 'S4' THEN (severity_label_age_in_days - 120)
        ELSE 0
      END)
    END                                                                                                                                                                                AS days_past_due,
    IFF(dates.date_actual >= DATE_TRUNC('day', issues.issue_moved_at), issues.issue_is_moved, FALSE)                                                                                   AS issue_is_moved,
    issues.epic_id,
    issues.epic_title,
    issues.epic_labels,
    issues.epic_state,
    issues.is_milestone_issue_reporting,
    issues.is_customer_related,
    issues.issue_type,
    issues.due_date,
    MAX(dates.date_actual) OVER ()                                                                                                                                                     AS last_updated_at,
    COALESCE(date_actual = last_updated_at, FALSE)                                                                                                                                     AS most_recent,
    ROW_NUMBER() OVER (
      PARTITION BY daily_issue_id
      ORDER BY LEAST(COALESCE(severity_label_valid_to, CURRENT_DATE), COALESCE(team_label_valid_to, CURRENT_DATE)), COALESCE(workflow_label_valid_to, CURRENT_DATE)
    )                                                                                                                                                                                  AS rn
  FROM issues
  INNER JOIN dates
    ON COALESCE(DATE_TRUNC('day', issues.created_at), dates.date_actual) <= dates.date_actual
      AND COALESCE(DATE_TRUNC('day', issues.closed_at), dates.date_actual) >= dates.min_date_actual
  LEFT JOIN assigend_users
    ON issues.issue_id = assigend_users.issue_id
  LEFT JOIN label_groups AS severity
    ON severity.label_type = 'severity'
      AND issues.issue_id = severity.issue_id
      AND dates.date_actual BETWEEN DATE_TRUNC('day', severity.label_valid_from) AND DATE_TRUNC('day', severity.label_valid_to)
  LEFT JOIN label_groups AS team
    ON team.label_type = 'team'
      AND issues.issue_id = team.issue_id
      AND dates.date_actual BETWEEN DATE_TRUNC('day', team.label_valid_from) AND DATE_TRUNC('day', team.label_valid_to)
  LEFT JOIN label_groups AS wf
    ON wf.label_type = 'workflow'
      AND issues.issue_id = wf.issue_id
      AND dates.date_actual BETWEEN DATE_TRUNC('day', wf.label_valid_from) AND DATE_TRUNC('day', wf.label_valid_to)
  LEFT JOIN workflow_labels AS wfl
    ON wf.workflow = wfl.workflow_label
)

SELECT * EXCLUDE rn
FROM final
WHERE rn = 1
