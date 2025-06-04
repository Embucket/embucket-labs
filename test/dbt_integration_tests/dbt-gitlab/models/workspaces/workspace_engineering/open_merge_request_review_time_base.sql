WITH internal_mrs AS (

  SELECT * EXCLUDE (merge_request_description)
  FROM {{ ref('internal_merge_requests_enhanced') }}
  WHERE created_at >= DATEADD(MONTH, -36, CURRENT_DATE)

),

map_employee_info AS (

  SELECT
    a.*,
    b.user_id
  FROM {{ ref('mart_team_member_directory') }} AS a
  INNER JOIN {{ ref('map_team_member_user') }} AS b ON a.employee_id = b.employee_id

),

date_spine AS (

  SELECT date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual BETWEEN DATEADD(MONTH, -36, CURRENT_DATE) AND CURRENT_DATE

),

/*add flag for mrs older than 365 days*/
add_old_flag AS (

  SELECT
    date_actual,
    internal_mrs.*,
    CASE WHEN DATEADD('day', -365, date_actual) >= created_at AND merged_at IS NULL THEN 1 ELSE 0 END AS old_1yr_flag,
    DATEDIFF('day', created_at, date_spine.date_actual)                                               AS days_open,
    ROUND(DATEDIFF('day', created_at, first_review_date), 2)                                          AS days_to_review,
    ROUND(DATEDIFF('day', first_review_date, date_spine.date_actual), 2)                              AS days_in_review,
    PERCENT_RANK() OVER (PARTITION BY date_actual ORDER BY days_open)                                 AS days_open_p95
  FROM date_spine
  INNER JOIN internal_mrs ON date_spine.date_actual BETWEEN DATE_TRUNC('day', internal_mrs.first_review_date) AND COALESCE(merged_at, last_reported_date, CURRENT_DATE)::DATE

),

final AS (
  SELECT DISTINCT
    mr.*,
    TRY_CAST(sizes.product_merge_request_files_changed AS INTEGER) AS files_changed,
    TRY_CAST(sizes.product_merge_request_lines_added AS INTEGER)   AS added_lines,
    TRY_CAST(sizes.product_merge_request_lines_removed AS INTEGER) AS removed_lines,
    map_employee_info.gitlab_username                              AS author_gitlab_username,
    map_employee_info.division                                     AS author_division,
    map_employee_info.department                                   AS author_department,
    map_employee_info.position                                     AS author_position,
    map_employee_info.job_specialty_single                         AS author_job_specialty
  FROM add_old_flag AS mr
  LEFT JOIN {{ ref('sizes_part_of_product_merge_requests') }} AS sizes ON mr.merge_request_iid = sizes.product_merge_request_iid AND mr.target_project_id = sizes.product_merge_request_project_id
  LEFT JOIN map_employee_info ON mr.author_id = map_employee_info.user_id AND DATE_TRUNC('day', mr.created_at) BETWEEN map_employee_info.valid_from AND DATEADD('day', -1, map_employee_info.valid_to)
  WHERE mr.merge_request_id NOT IN (SELECT deleted_merge_request_id FROM {{ ref('sheetload_deleted_mrs') }})
--AND old_1yr_flag = 0
-- and date_actual = dateadd('day',-1,current_date)
-- and reviewer_count is null
)

SELECT *
FROM final
