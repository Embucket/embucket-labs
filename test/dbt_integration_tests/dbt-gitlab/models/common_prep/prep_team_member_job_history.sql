{{ simple_cte([
    ('dates', 'dim_date'),
    ('employment_dates', 'fct_team_member_history'),
    ('staffing_history_approved_source', 'staffing_history_approved_source'),
    ('bamboohr_job_info','bamboohr_job_info'),
    ('bhr_job_role','bamboohr_job_role'),
    ('job_profiles','dim_job_profiles'),

]) }},

staffing_history AS (

  SELECT
    *,
    COALESCE(LAG(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date DESC, date_time_initiated DESC), '2099-01-01') AS next_effective_date
  FROM staffing_history_approved_source

),

bhr_job_info AS (

  SELECT *
  FROM bamboohr_job_info
  -- Ensure we only get information from people after they have been hired and before Workday went live
  WHERE effective_date < '2022-06-16'

),


bhr_job_info_historical AS (
  SELECT
    bhr_job_info.employee_id,
    bhr_job_info.job_title,
    IFF(
      bhr_job_info.job_title = 'Manager, Field Marketing',
      'Leader',
      COALESCE(bhr_job_role.job_role, bhr_job_info.job_role)
    )   AS job_role,
    CASE WHEN bhr_job_info.job_title = 'Group Manager, Product'
        THEN '9.5'
      WHEN bhr_job_info.job_title = 'Manager, Field Marketing'
        THEN '8'
      ELSE bhr_job_role.job_grade
    END AS job_grade,
    ROW_NUMBER() OVER (
      PARTITION BY bhr_job_info.employee_id ORDER BY dates.date_actual
    )   AS job_grade_event_rank
  FROM dates
  LEFT JOIN bhr_job_info
    ON
      dates.date_actual BETWEEN bhr_job_info.effective_date AND COALESCE(
        bhr_job_info.effective_end_date, DATE_TRUNC('week', DATEADD(WEEK, 3, CURRENT_DATE))
      )
  LEFT JOIN bhr_job_role
    ON bhr_job_info.employee_id = bhr_job_role.employee_id
      AND dates.date_actual BETWEEN bhr_job_role.effective_date AND COALESCE(
        bhr_job_role.next_effective_date, DATE_TRUNC('week', DATEADD(WEEK, 3, CURRENT_DATE))
      )
  WHERE bhr_job_role.job_grade IS NOT NULL
  -- Using the 1st instance we captured job_role and grade
  -- to identify classification for historical records
  QUALIFY job_grade_event_rank = 1

),

job_stage AS (

  SELECT
    employment_dates.employee_id,
    dates.date_actual,
    employment_dates.hire_date,
    employment_dates.hire_rank_asc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AS hire_rank,
    employment_dates.term_date,
    employment_dates.last_date,
    staffing_history.job_workday_id_current                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS job_profile_workday_id,
    job_profiles.job_code,
    CASE
      WHEN staffing_history.does_job_title_current_match_profile = 'TRUE' THEN job_profiles.job_profile
      WHEN staffing_history.does_job_title_current_match_profile = 'FALSE' THEN staffing_history.job_title_current
      ELSE bhr_job_info.job_title
    END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AS adj_job_title,
    CASE
      WHEN staffing_history.does_business_title_current_match_profile = 'TRUE' THEN job_profiles.job_profile
      WHEN staffing_history.does_business_title_current_match_profile = 'FALSE' THEN staffing_history.business_title_current
      ELSE bhr_job_info.job_title
    END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AS adj_business_title,
    COALESCE(job_profiles.management_level, bhr_job_info_historical.job_role, bhr_job_role.job_role, bhr_job_info.job_role)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS adj_job_role,
    COALESCE(job_profiles.job_level::VARCHAR, bhr_job_info_historical.job_grade, bhr_job_role.job_grade::VARCHAR)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 AS adj_job_grade,
    job_profiles.job_family,
    COALESCE(staffing_history.job_specialty_single_current, bhr_job_role.jobtitle_speciality)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AS adj_job_speciality_single,
    COALESCE(staffing_history.job_specialty_multi_current, bhr_job_role.jobtitle_speciality)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS adj_job_specialty_multi,
    {{ dbt_utils.generate_surrogate_key(['employment_dates.employee_id','employment_dates.hire_rank_asc','staffing_history.job_workday_id_current','job_profiles.job_code','adj_job_title','adj_business_title','adj_job_role','adj_job_grade','job_profiles.job_family','adj_job_speciality_single','adj_job_specialty_multi']) }} AS job_unique_key,
    COALESCE(LEAD(job_unique_key::TEXT) OVER (PARTITION BY employment_dates.employee_id, employment_dates.hire_rank_asc ORDER BY dates.date_actual DESC), job_unique_key)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         AS pr_job_unique_key
  FROM dates
  INNER JOIN employment_dates
    ON dates.date_actual >= employment_dates.hire_date
      AND dates.date_actual <= employment_dates.last_date
  LEFT JOIN staffing_history
    ON dates.date_actual >= staffing_history.effective_date
      AND dates.date_actual < staffing_history.next_effective_date
      AND employment_dates.employee_id = staffing_history.employee_id
      --To ensure that we capture information from people after they have been hired after Workday went live
      AND dates.date_actual >= '2022-06-16'::DATE
  LEFT JOIN bhr_job_info
    ON dates.date_actual >= bhr_job_info.effective_date
      -- Ensure we only get information from people after they have been hired and before Workday went live
      AND dates.date_actual <= LEAST(COALESCE(bhr_job_info.effective_end_date, '2022-06-15'), '2022-06-15')
      AND employment_dates.employee_id = bhr_job_info.employee_id
  LEFT JOIN bhr_job_info_historical
    ON employment_dates.employee_id = bhr_job_info_historical.employee_id
      AND bhr_job_info.job_title = bhr_job_info_historical.job_title
      -- to capture speciality for engineering prior to 2020.09.30 
      -- we are using sheetload, and capturing from bamboohr afterwards
      AND dates.date_actual BETWEEN '2019-11-01' AND '2020-02-27'
  LEFT JOIN bhr_job_role
    ON dates.date_actual >= bhr_job_role.effective_date
      -- Ensure we only get information from people after they have been hired and before Workday went live
      AND dates.date_actual <= COALESCE(bhr_job_role.next_effective_date, '2022-06-16')
      AND employment_dates.employee_id = bhr_job_role.employee_id
      -- Ensure we only get information from people after they have been hired and before Workday went live
      AND dates.date_actual < '2022-06-16'
  LEFT JOIN job_profiles
    ON staffing_history.job_workday_id_current = job_profiles.job_workday_id
      AND dates.date_actual >= job_profiles.valid_from
      AND dates.date_actual < job_profiles.valid_to
  QUALIFY pr_job_unique_key != job_unique_key OR employment_dates.hire_date = dates.date_actual
),

job_history_stage AS (

  SELECT
    employee_id                                                                              AS jh_employee_id,
    hire_rank                                                                                AS jh_hire_rank,
    date_actual                                                                              AS job_start_date,
    adj_job_title                                                                            AS cur_job_title,
    LEAD(adj_job_title) OVER (PARTITION BY employee_id, hire_rank ORDER BY date_actual DESC) AS pr_job_title,
    last_date                                                                                AS max_date
  FROM job_stage
  QUALIFY pr_job_title != cur_job_title OR hire_date = date_actual

),

job_history AS (

  SELECT
    jh_employee_id,
    jh_hire_rank,
    job_start_date,
    cur_job_title,
    COALESCE(LAG(job_start_date) OVER (PARTITION BY jh_employee_id, jh_hire_rank ORDER BY job_start_date DESC) - 1, max_date) AS job_end_date
  FROM job_history_stage


),

final AS (

  SELECT
    -- Primary keys
    {{ dbt_utils.generate_surrogate_key(['job_stage.employee_id', 'job_stage.job_profile_workday_id']) }} AS team_member_job_pk,
    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['job_stage.employee_id'])) }}                                                                                                                      AS dim_team_member_sk,

    -- Legacy Keys  
    job_stage.employee_id,
    job_stage.job_profile_workday_id,

    -- Team member job history attributes
    job_stage.date_actual                                                                                                                                                                                   AS valid_from,
    COALESCE(LAG(job_stage.date_actual) OVER (PARTITION BY job_stage.employee_id, job_stage.hire_rank ORDER BY job_stage.date_actual DESC) - 1, job_stage.last_date)                                        AS valid_to,
    job_stage.hire_rank,
    job_stage.job_unique_key                                                                                                                                                                                AS job_combination_id,
    job_stage.job_code,
    job_stage.job_family,
    job_stage.adj_job_title                                                                                                                                                                                 AS job_title,
    job_stage.adj_business_title                                                                                                                                                                            AS business_title,
    job_stage.adj_job_role                                                                                                                                                                                  AS job_role,
    job_stage.adj_job_grade                                                                                                                                                                                 AS job_grade,
    job_stage.adj_job_speciality_single                                                                                                                                                                     AS job_specialty_single,
    job_stage.adj_job_specialty_multi                                                                                                                                                                       AS job_specialty_multi,
    job_history.job_start_date,
    job_history.job_end_date,
    IFF(ROW_NUMBER() OVER (PARTITION BY job_stage.employee_id ORDER BY job_stage.date_actual DESC) = 1, 'TRUE', 'FALSE')                                                                                    AS is_last_record,
    IFF(ROW_NUMBER() OVER (PARTITION BY job_stage.employee_id, job_stage.hire_rank ORDER BY job_stage.date_actual DESC) = 1, 'TRUE', 'FALSE')                                                               AS is_last_employment_record,
    IFF(valid_to = last_date AND term_date IS NULL, 'TRUE', 'FALSE')                                                                                                                                        AS is_active_team_member
  FROM job_stage
  LEFT JOIN job_history
    ON job_stage.employee_id = job_history.jh_employee_id
      AND job_stage.hire_rank = job_history.jh_hire_rank
      AND job_stage.date_actual BETWEEN job_history.job_start_date AND job_history.job_end_date

)

SELECT *
FROM final
