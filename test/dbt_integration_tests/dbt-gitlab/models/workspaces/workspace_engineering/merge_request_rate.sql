WITH merged_merge_requests AS (

  SELECT *
  FROM {{ ref('engineering_merge_requests') }}
  WHERE merged_at IS NOT NULL
    AND merged_at >= '2020-01-01'

),

team_member_history AS (

  SELECT
    *,
    DATE_TRUNC('month', date_actual) AS employee_month
  FROM {{ ref('team_member_history') }}
  WHERE date_actual <= CURRENT_DATE
    AND date_actual >= '2020-01-01'
    AND department != 'CEO'
    AND division = 'Engineering'

),

team_author_product_mrs AS ( --this is used to calculate department MR rate outside Development; for Development we count all product MRs regardless of author

  SELECT
    merged_merge_requests.merge_month,
    team_member_history.department,
    COUNT(DISTINCT merged_merge_requests.merge_request_id) AS mrs
  FROM merged_merge_requests
  INNER JOIN team_member_history ON merged_merge_requests.author_id = team_member_history.user_id AND merged_merge_requests.merge_month = team_member_history.employee_month
  GROUP BY 1, 2

),

aggregated AS (

  SELECT
    merged_merge_requests.merge_month,
    merged_merge_requests.group_label                                                                                                AS group_name,
    ''                                                                                                                               AS department,
    technology_group,
    COUNT(DISTINCT team_member_history.employee_id)                                                                                  AS employees,
    COUNT(DISTINCT IFF(merged_merge_requests.author_id = team_member_history.user_id, merged_merge_requests.merge_request_id, NULL)) AS mrs,
    ROUND(mrs / NULLIF(employees, 0), 2)                                                                                             AS mr_rate,
    'group and tech group'                                                                                                           AS granularity_level
  FROM merged_merge_requests
  INNER JOIN team_member_history
    ON merged_merge_requests.merge_month = team_member_history.employee_month
      AND merged_merge_requests.group_label = team_member_history.user_group
  WHERE team_member_history.division = 'Engineering'
  GROUP BY ALL

  UNION ALL

  SELECT
    merged_merge_requests.merge_month,
    merged_merge_requests.group_label                      AS group_name,
    ''                                                     AS department,
    ''                                                     AS technology_group,
    COUNT(DISTINCT team_member_history.employee_id)        AS employees,
    COUNT(DISTINCT merged_merge_requests.merge_request_id) AS mrs,
    ROUND(mrs / NULLIF(employees, 0), 2)                   AS mr_rate,
    'group'                                                AS granularity_level
  FROM merged_merge_requests
  LEFT JOIN team_member_history ON merged_merge_requests.merge_month = team_member_history.employee_month AND merged_merge_requests.group_label = team_member_history.user_group
  WHERE team_member_history.division = 'Engineering'
  GROUP BY ALL

  UNION ALL

  SELECT
    team_author_product_mrs.merge_month,
    ''                                              AS group_name,
    team_author_product_mrs.department,
    ''                                              AS technology_group,
    COUNT(DISTINCT team_member_history.employee_id) AS employees,
    team_author_product_mrs.mrs,
    ROUND(mrs / NULLIF(employees, 0), 2)            AS mr_rate,
    'department'                                    AS granularity_level
  FROM team_author_product_mrs
  LEFT JOIN team_member_history ON team_author_product_mrs.merge_month = team_member_history.employee_month AND team_author_product_mrs.department = team_member_history.department
  WHERE team_member_history.division = 'Engineering'
  AND team_author_product_mrs.department != 'Development'
  GROUP BY ALL

  UNION ALL

  SELECT
    merged_merge_requests.merge_month,
    ''                                                        AS group_name,
    'Development' AS department,
    ''                                                        AS technology_group,
    COUNT(DISTINCT team_member_history.employee_id) AS employees,
    COUNT(DISTINCT merged_merge_requests.merge_request_id)    AS mrs,
    ROUND(mrs / (NULLIF(employees, 0) - 3), 2)                AS mr_rate,
    'department'                                              AS granularity_level
  FROM merged_merge_requests
  LEFT JOIN team_member_history ON merged_merge_requests.merge_month = team_member_history.employee_month 
  WHERE team_member_history.division = 'Engineering'
  AND  department in ('Core Development','Expansion','Development')
  GROUP BY ALL

)

SELECT *
FROM aggregated
