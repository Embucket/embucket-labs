WITH issues AS (

  SELECT *
  FROM {{ ref('engineering_issues') }}

),

label_links AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_label_links') }}

),

labels AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_labels_xf') }}

),

incidents AS (
  SELECT DISTINCT
    issues.issue_iid AS issue_iid, 
    issues.issue_id AS issue_id,
    label_links.target_id AS label_target_id,
    issue_title,
    CASE
      WHEN issues.masked_label_title LIKE '%severity::1%' 
        THEN 'S1'
      WHEN issues.masked_label_title LIKE '%severity::2%' 
        THEN 'S2' 
      WHEN issues.masked_label_title LIKE '%severity::3%' 
        THEN 'S3'
      WHEN issues.masked_label_title LIKE '%severity::4%' 
        THEN 'S4'
      ELSE 'No Severity'
    END AS severity,
    issues.closed_at::date AS closed_at,
    issues.created_at::date AS created_at,
    label_links.label_link_created_at AS label_added_at,
    label_links.label_link_updated_at AS label_updated_at,
    valid_from,
    valid_to,
    labels.label_id,
    labels.masked_label_title,
    CASE
      WHEN LOWER(labels.masked_label_title) = 'incident'
        THEN 1
      ELSE 0
    END AS is_incident,
    CASE
      WHEN LOWER(labels.masked_label_title) = 'incident::resolved'
        THEN 1
      ELSE 0
    END AS is_resolved_incident,
    CASE
      WHEN LOWER(labels.masked_label_title) = 'incident::mitigated'
        THEN 1
      ELSE 0
    END AS is_mitigated_incident,
    CASE
      WHEN LOWER(labels.masked_label_title) = 'incident::resolved'
        THEN DATEDIFF('min', created_at, label_added_at) / 60
      ELSE NULL
    END AS time_to_mitigate_resolved,
    CASE
      WHEN LOWER(labels.masked_label_title) = 'incident::mitigated'
        THEN DATEDIFF('min', created_at, label_added_at) / 60
      ELSE NULL
    END AS time_to_mitigate_mitigated
  FROM issues
  INNER JOIN label_links ON issues.issue_id = label_links.target_id
  INNER JOIN labels ON label_links.label_id = labels.label_id
  WHERE issues.project_id = '7444821'
  AND (LOWER(labels.masked_label_title) IN ('incident','incident::resolved','incident::mitigated'))
  AND issues.severity_label IN ('severity 1','severity 2')
  AND DATE_TRUNC('month',created_at) >= DATEADD('month',-36,current_date)
  
),

final AS (

  SELECT DISTINCT
    'monthly' AS granularity_level,
    DATE_TRUNC('month', created_at) AS date,
    SUM(is_incident) AS incidents,
    SUM(is_resolved_incident) AS resolved_incidents,
    SUM(is_mitigated_incident) AS mitigated_incidents,
    AVG(time_to_mitigate_resolved) AS mttr,
    AVG(time_to_mitigate_mitigated) AS mttm
  FROM incidents
  GROUP BY 1,2

  UNION

  SELECT DISTINCT
    'daily' AS granularity_level,
    DATE_TRUNC('day', created_at) AS date,
    SUM(is_incident) AS incidents,
    SUM(is_resolved_incident) AS resolved_incidents,
    SUM(is_mitigated_incident) AS mitigated_incidents,
    AVG(time_to_mitigate_resolved) AS mttr,
    AVG(time_to_mitigate_mitigated) AS mttm
  FROM incidents
  GROUP BY 1,2
  
)

SELECT *
FROM final
