WITH bugs AS (
  SELECT DISTINCT
    DATE_TRUNC('month', date_actual) AS month,
    issue_id,
    stage_label,
    group_label,
    section_label
  FROM {{ ref('issues_history') }} --using this instead of internal_issues_enhanced because severity labels are subject to change during issue's life time
  WHERE is_part_of_product
    AND type_label = 'bug'
    AND severity != 'No Severity'
    AND issue_is_moved = FALSE
    AND month = DATE_TRUNC('month', issue_created_at)
),

product_mrs AS (
  SELECT
    DATE_TRUNC('month', merged_at) AS month,
    merge_request_id,
    stage_label,
    group_label,
    section_label
  FROM {{ ref('engineering_merge_requests') }}
  WHERE merged_at IS NOT NULL
    AND merged_at BETWEEN DATEADD('month', -24, CURRENT_DATE) AND CURRENT_DATE
    AND is_created_by_bot = FALSE
),

bug_regroup AS (
  SELECT
    month,
    'group'         AS breakout,
    group_label,
    ''              AS stage_label,
    ''              AS section_label,
    COUNT(issue_id) AS issues
  FROM bugs
  {{ dbt_utils.group_by(n=5) }}
  UNION ALL
  SELECT
    month,
    'stage'         AS breakout,
    ''              AS group_label,
    stage_label,
    ''              AS section_label,
    COUNT(issue_id) AS issues
  FROM bugs
  {{ dbt_utils.group_by(n=5) }}
  UNION ALL
  SELECT
    month,
    'section'       AS breakout,
    ''              AS group_label,
    ''              AS stage_label,
    section_label,
    COUNT(issue_id) AS issues
  FROM bugs
  {{ dbt_utils.group_by(n=5) }}
),

mr_regroup AS (
  SELECT
    month,
    'group'                 AS breakout,
    group_label,
    ''                      AS stage_label,
    ''                      AS section_label,
    COUNT(merge_request_id) AS mr
  FROM product_mrs
  {{ dbt_utils.group_by(n=5) }}
  UNION ALL
  SELECT
    month,
    'stage'                 AS breakout,
    ''                      AS group_label,
    stage_label,
    ''                      AS section_label,
    COUNT(merge_request_id) AS mr
  FROM product_mrs
  {{ dbt_utils.group_by(n=5) }}
  UNION ALL
  SELECT
    month,
    'section'               AS breakout,
    ''                      AS group_label,
    ''                      AS stage_label,
    section_label,
    COUNT(merge_request_id) AS mr
  FROM product_mrs
  {{ dbt_utils.group_by(n=5) }}
)

SELECT
  COALESCE(a.month, b.month)                 AS month,
  COALESCE(a.breakout, b.breakout)           AS breakout,
  COALESCE(a.group_label, b.group_label)     AS group_label,
  COALESCE(a.stage_label, b.stage_label)     AS stage_label,
  COALESCE(a.section_label, b.section_label) AS section_label,
  COALESCE(issues, 0)                        AS issues,
  COALESCE(mr, 0)                            AS mr
FROM bug_regroup AS a
FULL OUTER JOIN
  mr_regroup
    AS b
  ON a.month = b.month
    AND a.breakout = b.breakout
    AND a.group_label = b.group_label
    AND a.stage_label = b.stage_label
    AND a.section_label = b.section_label
UNION ALL
SELECT
  a.month,
  'all'                            AS breakout,
  ''                               AS group_label,
  ''                               AS stage_label,
  ''                               AS section_label,
  COUNT(DISTINCT issue_id)         AS issues,
  COUNT(DISTINCT merge_request_id) AS mr
FROM bugs AS a
INNER JOIN product_mrs AS b ON a.month = b.month
GROUP BY 1
