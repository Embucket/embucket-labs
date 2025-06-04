WITH team_member AS (

  SELECT DISTINCT
    dim_team_member_sk,
    employee_id,
    nationality,
    ethnicity,
    first_name,
    last_name,
    full_name,
    gender,
    work_email,
    date_of_birth,
    age_calculated,
    age_cohort,
    gitlab_username,
    team_id,
    country,
    region,
    region_modified,
    gender_region,
    ethnicity_region,
    urg_group,
    urg_region,
    hire_date,
    employee_type,
    termination_date,
    is_current_team_member,
    is_rehire,
    valid_from,
    valid_to
  FROM {{ ref('dim_team_member') }}

),

team_status AS (
    
    SELECT
    dim_team_member_sk   AS dim_team_member_sk,
    dim_team_sk          AS dim_team_sk,
    employee_id          AS employee_id,
    team_id              AS team_id,
    job_code             AS job_code,
    position             AS position,
    job_family           AS job_family,
    job_specialty_single AS job_specialty_single,
    job_specialty_multi  AS job_specialty_multi,
    management_level     AS management_level,
    job_grade            AS job_grade,
    entity               AS entity,
    is_position_active,
    is_current AS is_current_team_member_position,
    valid_from AS position_valid_from,
    valid_to   AS position_valid_to
    FROM {{ ref('fct_team_status') }}

),

team_member_absence AS (

  SELECT
    dim_team_member_sk   AS dim_team_member_sk,
    dim_team_sk          AS dim_team_sk,
    employee_id          AS employee_id,
    absence_date,
    absence_start,
    absence_end,
    is_pto,
    is_holiday,
    pto_uuid,
    pto_type_uuid,
    pto_status,
    pto_status_name,
    pto_group_type,
    total_hours,
    recorded_hours,
    absence_status,
    employee_day_length
  FROM {{ ref('fct_team_member_absence') }}
),

final AS (
  SELECT DISTINCT
    -- Surrogate keys
    team_member.dim_team_member_sk,
    team_member_absence.dim_team_sk,

    --Natural keys
    team_member.employee_id,

    --Team member info
    team_member.nationality,
    team_member.ethnicity,
    team_member.first_name,
    team_member.last_name,
    team_member.full_name,
    team_member.gender,
    team_member.work_email,
    team_member.date_of_birth,
    team_member.gitlab_username,
    team_member.country,
    team_member.region,
    team_member.region_modified,
    team_member.gender_region,
    team_member.ethnicity_region,
    team_member.urg_group,
    team_member.urg_region,
    team_member.hire_date,
    team_member.employee_type,
    team_member.termination_date,
    team_member.is_current_team_member,
    team_member.is_rehire,
    team_member.team_id,
    team_status.job_code,
    team_status.position,
    team_status.entity,
    team_status.job_family,
    team_status.job_specialty_single,
    team_status.job_specialty_multi,
    team_status.management_level,
    team_status.job_grade,
    team_status.is_position_active,
    team_status.is_current_team_member_position,
    team_member_absence.absence_date,
    team_member_absence.absence_start,
    team_member_absence.absence_end,
    team_member_absence.is_pto,
    team_member_absence.is_holiday,
    team_member_absence.pto_uuid,
    team_member_absence.pto_type_uuid,
    team_member_absence.pto_status,
    team_member_absence.pto_status_name,
    team_member_absence.pto_group_type,
    team_member_absence.total_hours,
    team_member_absence.recorded_hours,
    team_member_absence.absence_status,
    team_member_absence.employee_day_length
  FROM team_member_absence
  INNER JOIN team_member
    ON team_member_absence.employee_id = team_member.employee_id
      AND NOT (
        team_member_absence.absence_date >= team_member.valid_to
        OR team_member_absence.absence_date <= team_member.valid_from
      )
  LEFT JOIN team_status 
    ON team_status.employee_id = team_member_absence.employee_id
    AND NOT (
      team_status.position_valid_to <= team_member_absence.absence_date
      OR team_status.position_valid_from >= team_member_absence.absence_date
    )
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@rakhireddy",
    updated_by="@rakhireddy",
    created_date="2024-05-15",
    updated_date="2024-08-05"
) }}
