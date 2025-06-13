WITH job_info_source AS (

  SELECT *
  FROM {{ref('blended_job_info_source')}}
  WHERE source_system = 'bamboohr'
    AND effective_date < '2022-06-16'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY effective_date DESC) = 1

),

employee_mapping AS (

  SELECT *
  FROM {{ref('blended_employee_mapping_source')}}
  WHERE source_system = 'bamboohr'
    --ensure we only get information from people after they have been hired and before Workday went live
    AND uploaded_at < '2022-06-16'
      AND uploaded_at > hire_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, uploaded_at ORDER BY uploaded_at DESC) = 1 

),

staffing_history AS (

  SELECT *,
    COALESCE(LAG(effective_date) OVER ( PARTITION BY employee_id ORDER BY effective_date DESC,date_time_initiated DESC ), '2099-01-01') AS next_effective_date
  FROM {{ref('staffing_history_approved_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY date_time_initiated DESC) = 1 

),

job_profiles AS (

  SELECT 
    job_workday_id,
    job_code,
    job_profile                                                                                                                AS position,
    job_family                                                                                                                 AS job_family,
    management_level                                                                                                           AS management_level, 
    job_level                                                                                                                  AS job_grade,
    is_job_profile_active                                                                                                      AS is_position_active,
    valid_from,
    valid_to
  FROM {{ref('blended_job_profiles_source')}}

),

dates AS (

  SELECT *
  FROM {{ref('dim_date')}}

),

cost_centers AS (

  SELECT *
  FROM {{ref('cost_centers_source')}}

),

team_info AS (

  -- Get team, department, division info from the BambooHR data
  -- Solve for gaps and islands problem in data

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'job_title', 'department', 'division', 'reports_to','entity']) }}               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_title                                                                                                                  AS position,
    reports_to                                                                                                                 AS manager,
    entity                                                                                                                     AS entity, 
    department                                                                                                                 AS department,
    division                                                                                                                   AS division,
    effective_date                                                                                                             AS effective_date,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY effective_date)                                           AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER ( PARTITION BY employee_id ORDER BY effective_date)              AS unique_key_group 
  FROM job_info_source

),

team_info_group AS (

  -- Combine the team_info and job_profiles data to get some data that didn't exist in BHR 
  -- Group by unique_key_group to clearly identify the different team change events

  SELECT 
    team_info.employee_id, 
    team_info.position,
    team_info.manager,
    team_info.entity, 
    team_info.department,
    team_info.division,
    team_info.unique_key_group,
    MIN(team_info.effective_date) AS effective_date
  FROM team_info
  {{ dbt_utils.group_by(n=7)}}

),

job_info AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'job_role', 'job_grade', 'jobtitle_speciality_single_select', 'jobtitle_speciality_multi_select','termination_date']) }}
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_role                                                                                                                   AS management_level,
    job_grade                                                                                                                  AS job_grade, 
    jobtitle_speciality_single_select                                                                                          AS job_specialty_single,
    jobtitle_speciality_multi_select                                                                                           AS job_specialty_multi,
    termination_date                                                                                                           AS termination_date,
    uploaded_at                                                                                                                AS effective_date,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                                              AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                  AS unique_key_group 
  FROM employee_mapping
  

),

job_info_group AS (

  -- Group by unique_key_group to clearly identify the different job change events

  SELECT 
    employee_id, 
    management_level,
    job_grade,
    job_specialty_single, 
    job_specialty_multi,
    termination_date, 
    unique_key_group,
    MIN(effective_date) AS effective_date
  FROM job_info
  {{ dbt_utils.group_by(n=7)}}
  
),

legacy_data AS (

SELECT
  
    team_info_group.employee_id                                                                                                AS employee_id,
    team_info_group.manager                                                                                                    AS manager,
    team_info_group.department                                                                                                 AS department,
    team_info_group.division                                                                                                   AS division,
    NULL                                                                                                                       AS job_specialty_single,
    NULL                                                                                                                       AS job_specialty_multi,
    team_info_group.entity                                                                                                     AS entity,
    team_info_group.position                                                                                                   AS position,
    NULL                                                                                                                       AS management_level,
    NULL                                                                                                                       AS job_grade,
    NULL                                                                                                                       AS termination_date,
    team_info_group.effective_date                                                                                             AS effective_date
  FROM team_info_group

UNION

  SELECT
    job_info_group.employee_id                                                                                                 AS employee_id,
    NULL                                                                                                                       AS manager,
    NULL                                                                                                                       AS department,
    NULL                                                                                                                       AS division,
    job_info_group.job_specialty_single                                                                                        AS job_specialty_single,
    job_info_group.job_specialty_multi                                                                                         AS job_specialty_multi,
    NULL                                                                                                                       AS entity,
    NULL                                                                                                                       AS position,
    job_info_group.management_level                                                                                            AS management_level,
    job_info_group.job_grade                                                                                                   AS job_grade,
    job_info_group.termination_date                                                                                            AS termination_date,
    job_info_group.effective_date                                                                                              AS effective_date
  FROM job_info_group

),

legacy_clean AS (

  SELECT 
    legacy_data.employee_id                                                                                                    AS employee_id,
    LAST_VALUE(legacy_data.manager IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)              
                                                                                                                               AS manager,
    LAST_VALUE(legacy_data.position IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS position,
    LAST_VALUE(legacy_data.job_specialty_single IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS job_specialty_single,
    LAST_VALUE(legacy_data.job_specialty_multi IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS job_specialty_multi,
    LAST_VALUE(legacy_data.management_level IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                           
                                                                                                                               AS management_level,
    LAST_VALUE(legacy_data.job_grade IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS job_grade,
    LAST_VALUE(legacy_data.department IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS department,
    LAST_VALUE(legacy_data.division IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS division,
    LAST_VALUE(legacy_data.entity IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS entity,
    LAST_VALUE(legacy_data.termination_date IGNORE NULLS) OVER (PARTITION BY legacy_data.employee_id ORDER BY legacy_data.effective_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                          
                                                                                                                               AS termination_date,
    legacy_data.effective_date
  FROM legacy_data

),

position_history AS (

  -- Combine workday tables to get an accurate picture of the team members info

  SELECT
    staffing_history.employee_id                                                                                               AS employee_id,
    staffing_history.team_id_current                                                                                           AS team_id,
    staffing_history.manager_current                                                                                           AS manager,
    cost_centers.department_name                                                                                               AS department,
    cost_centers.division                                                                                                      AS division,
    staffing_history.suporg_current                                                                                            AS suporg,
    job_profiles.job_code                                                                                                      AS job_code,
    staffing_history.job_specialty_single_current                                                                              AS job_specialty_single,
    staffing_history.job_specialty_multi_current                                                                               AS job_specialty_multi,
    staffing_history.entity_current                                                                                            AS entity,
    IFF(business_process_type IN (
			'End Contingent Worker Contract'
			,'Termination'
			), staffing_history.effective_date, NULL)                                                                                AS termination_date,           
    job_profiles.position                                                                                                      AS position,
    job_profiles.job_family                                                                                                    AS job_family,
    job_profiles.management_level                                                                                              AS management_level,
    job_profiles.job_grade::VARCHAR                                                                                            AS job_grade,
    job_profiles.is_position_active                                                                                            AS is_position_active,
    MIN(dates.date_actual)                                                                                                    AS effective_date
  FROM dates
  INNER JOIN staffing_history
    ON dates.date_actual >= staffing_history.effective_date
      AND dates.date_actual < staffing_history.next_effective_date
  LEFT JOIN job_profiles
    ON staffing_history.job_workday_id_current = job_profiles.job_workday_id
      AND dates.date_actual >= job_profiles.valid_from
      AND dates.date_actual < job_profiles.valid_to
  LEFT JOIN cost_centers
    ON staffing_history.department_workday_id_current = cost_centers.department_workday_id
  WHERE dates.date_actual BETWEEN '2022-06-16' and CURRENT_DATE
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,staffing_history.hire_date

  UNION

  SELECT
    legacy_clean.employee_id                                                                                                   AS employee_id,
    NULL                                                                                                                       AS team_id,
    legacy_clean.manager                                                                                                       AS manager,
    legacy_clean.department                                                                                                    AS department,
    legacy_clean.division                                                                                                      AS division,
    NULL                                                                                                                       AS suporg,
    NULL                                                                                                                       AS job_code,
    job_specialty_single                                                                                                       AS job_specialty_single,
    job_specialty_multi                                                                                                        AS job_specialty_multi,
    legacy_clean.entity                                                                                                        AS entity,
    legacy_clean.termination_date                                                                                              AS termination_date,
    legacy_clean.position                                                                                                      AS position,
    NULL                                                                                                                       AS job_family,
    management_level                                                                                                           AS management_level,
    job_grade                                                                                                                  AS job_grade,
    NULL                                                                                                                       AS is_position_active,
    legacy_clean.effective_date                                                                                                AS effective_date
  FROM legacy_clean
  
),

union_clean AS (

  SELECT 
    position_history.employee_id                                                                                               AS employee_id,
    position_history.team_id                                                                                                   AS team_id,
    position_history.manager                                                                                                   AS manager,
    position_history.suporg                                                                                                    AS suporg,
    position_history.job_code                                                                                                  AS job_code,
    position_history.position                                                                                                  AS position,
    position_history.job_family                                                                                                AS job_family,
    position_history.job_specialty_single                                                                                      AS job_specialty_single,
    position_history.job_specialty_multi                                                                                       AS job_specialty_multi,
    position_history.management_level                                                                                          AS management_level,
    position_history.job_grade                                                                                                 AS job_grade,
    position_history.department                                                                                                AS department,
    position_history.division                                                                                                  AS division,
    position_history.entity                                                                                                    AS entity,
    position_history.termination_date                                                                                          AS termination_date,
    position_history.is_position_active                                                                                        AS is_position_active,
    position_history.effective_date                                                                                            AS effective_date
  FROM position_history

),

final AS (

  SELECT 
    -- Surrogate keys
    {{ dbt_utils.generate_surrogate_key(['union_clean.employee_id'])}}                                                         AS dim_team_member_sk,
    {{ dbt_utils.generate_surrogate_key(['union_clean.team_id'])}}                                                             AS dim_team_sk,

    -- Team member position attributes
    union_clean.employee_id                                                                                                    AS employee_id,
    union_clean.team_id                                                                                                        AS team_id,
    union_clean.manager                                                                                                        AS manager,
    union_clean.suporg                                                                                                         AS suporg,
    union_clean.job_code                                                                                                       AS job_code,
    union_clean.position                                                                                                       AS position,
    union_clean.job_family                                                                                                     AS job_family,
    union_clean.job_specialty_single                                                                                           AS job_specialty_single,
    union_clean.job_specialty_multi                                                                                            AS job_specialty_multi,
    union_clean.management_level                                                                                               AS management_level,
    union_clean.job_grade                                                                                                      AS job_grade,
    union_clean.department                                                                                                     AS department,
    union_clean.division                                                                                                       AS division,
    union_clean.entity                                                                                                         AS entity,
    union_clean.termination_date                                                                                               AS termination_date,
    union_clean.is_position_active                                                                                             AS is_position_active,
    union_clean.effective_date                                                                                                 AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY union_clean.employee_id ORDER BY valid_from)                   AS valid_to
  FROM union_clean

)

SELECT 
  dim_team_member_sk,                                                                                                              
  dim_team_sk,
  employee_id,
  team_id,
  manager,
  suporg,
  job_code,
  position,
  job_family,
  job_specialty_single,
  job_specialty_multi,
  management_level,
  job_grade,
  department,
  division,
  entity,
  is_position_active,
  DATE(valid_from)                                                                                                           AS valid_from,
  DATE(valid_to)                                                                                                             AS valid_to,
  CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY valid_from DESC) = 1 
      THEN TRUE
    ELSE FALSE
  END                                                                                                                        AS is_current
FROM final
WHERE termination_date IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, DATE(valid_from) ORDER BY valid_from DESC)  = 1 