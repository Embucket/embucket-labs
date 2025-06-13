WITH final AS (

  SELECT
    prep_team_member.dim_team_member_sk,
    prep_team_member.employee_id,
    prep_team_member.nationality,
    prep_team_member.ethnicity,
    prep_team_member.first_name,
    prep_team_member.last_name,
    prep_team_member.full_name,
    prep_team_member.gender,
    prep_team_member.work_email,
    prep_team_member.date_of_birth,
    prep_team_member.key_talent_status,
    prep_team_member.age_calculated,
    prep_team_member.age_cohort,
    prep_team_member.region_modified,
    prep_team_member.gender_region,
    prep_team_member.ethnicity_region,
    prep_team_member.urg_group,
    prep_team_member.urg_region,
    prep_team_member.gitlab_username,
    prep_team_member.growth_potential_rating,
    prep_team_member.performance_rating,
    prep_team_member.country,
    prep_team_member.region,
    prep_team_member.team_id,
    prep_team_member.employee_type,
    prep_team_member.hire_date,
    prep_team_member.termination_date,
    prep_team_member.is_current_team_member,
    prep_team_member.is_rehire,
    prep_team_member.valid_from,
    prep_team_member.valid_to,
    prep_team_member.is_current
  FROM {{ ref('prep_team_member') }}

  UNION ALL

  SELECT
    MD5('-1')                         AS dim_team_member_sk,
    -1                                AS employee_id,
    'Missing nationality'             AS nationality,
    'Missing ethnicity'               AS ethnicity,
    'Missing first_name'              AS first_name,
    'Missing last_name'               AS last_name,
    'Missing full_name'               AS full_name,
    'Missing gender'                  AS gender,
    'Missing work_email'              AS work_email,
    '9999-12-31 00:00:00.000 +0000'   AS date_of_birth,
    'Missing key_talent_status'       AS key_talent_status,
    -1                                AS age_calculated,
    'Missing age_cohort'              AS age_cohort,
    'Missing region_modified'         AS region_modified,
    'Missing gender_region'           AS gender_region,
    'Missing ethnicity_region'        AS ethnicity_region,
    NULL                              AS urg_group,
    'Missing urg_region'              AS urg_region,
    'Missing gitlab_username'         AS gitlab_username,
    'Missing growth_potential_rating' AS growth_potential_rating,
    'Missing performance_rating'      AS performance_rating,
    'Missing country'                 AS country,
    'Missing region'                  AS region,
    'Missing team_id'                 AS team_id,
    'Missing employee_type'           AS employee_type,
    '9999-12-31 00:00:00.000 +0000'   AS hire_date,
    '9999-12-31 00:00:00.000 +0000'   AS termination_date,
    NULL                              AS is_current_team_member,
    NULL                              AS is_rehire,
    '9999-12-31 00:00:00.000 +0000'   AS valid_from,
    '9999-12-31 00:00:00.000 +0000'   AS valid_to,
    NULL                              AS is_current

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@rakhireddy',
    created_date='2023-03-27',
    updated_date='2024-05-21'
) }}
