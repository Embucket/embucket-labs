WITH final AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'team_id', 'valid_from']) }}  AS team_member_position_pk,
    {{ get_keyed_nulls('prep_team_member_position.dim_team_member_sk') }} AS dim_team_member_sk,
    {{ get_keyed_nulls('prep_team_member_position.dim_team_sk') }} AS dim_team_sk,
    prep_team_member_position.employee_id,
    prep_team_member_position.team_id,
    prep_team_member_position.manager,
    prep_team_member_position.suporg,
    prep_team_member_position.job_code,
    prep_team_member_position.position,
    prep_team_member_position.job_family,
    prep_team_member_position.job_specialty_single,
    prep_team_member_position.job_specialty_multi,
    prep_team_member_position.management_level,
    prep_team_member_position.job_grade,
    prep_team_member_position.department,
    prep_team_member_position.division,
    prep_team_member_position.entity,
    prep_team_member_position.is_position_active,
    prep_team_member_position.valid_from,
    prep_team_member_position.valid_to,
    prep_team_member_position.is_current
  FROM {{ ref('prep_team_member_position') }}

)


{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-05-30',
    updated_date='2023-09-26'
) }}
