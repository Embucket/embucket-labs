WITH final AS (

  SELECT 
    prep_team_status.dim_team_member_sk,
    prep_team_status.dim_team_sk,
    prep_team_status.employee_id,
    prep_team_status.team_id,
    prep_team_status.job_code,
    prep_team_status.position,
    prep_team_status.job_family,
    prep_team_status.job_specialty_single,
    prep_team_status.job_specialty_multi,
    prep_team_status.management_level,
    prep_team_status.job_grade,
    prep_team_status.entity,
    prep_team_status.is_position_active,
    prep_team_status.employment_status,
    prep_team_status.termination_reason,
    prep_team_status.termination_type,
    prep_team_status.exit_impact,
    prep_team_status.valid_from,
    prep_team_status.valid_to,
    prep_team_status.is_current
  FROM {{ref('prep_team_status')}} 
   
)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-06-01',
    updated_date='2023-06-01'
) }}