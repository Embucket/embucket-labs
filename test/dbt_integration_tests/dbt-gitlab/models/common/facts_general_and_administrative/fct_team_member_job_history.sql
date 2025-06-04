WITH prep_team_member_job_history AS (
  SELECT
    -- Primary keys
    team_member_job_pk,
    -- Surrogate Keys
    dim_team_member_sk,

    -- Legacy Keys  
    employee_id,
    job_profile_workday_id,

    -- Team member job history attributes
    hire_rank,
    job_combination_id,
    job_code,
    job_family,
    job_title,
    business_title,
    job_role,
    job_grade,
    job_specialty_single,
    job_specialty_multi,
    is_last_record,
    is_last_employment_record,
    is_active_team_member,
    job_start_date,
    job_end_date,
    valid_from,
    valid_to
  FROM {{ ref('prep_team_member_job_history') }}
)

SELECT *
FROM prep_team_member_job_history
