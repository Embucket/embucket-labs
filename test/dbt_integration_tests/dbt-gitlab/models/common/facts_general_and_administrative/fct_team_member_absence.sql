WITH team_status_dup AS (
  SELECT *
  FROM {{ ref('fct_team_status') }}
),

pto AS (
  SELECT *
  FROM {{ ref('prep_pto') }}
),

team_status AS (
  SELECT
    dim_team_member_sk,
    dim_team_sk,
    employee_id,
    valid_from AS position_valid_from,
    valid_to   AS position_valid_to
  FROM team_status_dup
),

combined_sources AS (
  SELECT
  distinct
    team_status.dim_team_member_sk,
    team_status.dim_team_sk,
    team_status.employee_id,
    pto.start_date         AS absence_start,
    pto.end_date           AS absence_end,
    pto.pto_date           AS absence_date,
    pto.is_pto             AS is_pto,
    pto.is_holiday         AS is_holiday,
    pto.pto_uuid           AS pto_uuid,
    pto.pto_type_uuid      AS pto_type_uuid,
    pto.pto_group_type     AS pto_group_type,
    pto.pto_status         AS pto_status,
    pto.pto_status_name    AS pto_status_name,
    pto.total_hours        AS total_hours,
    pto.recorded_hours     AS recorded_hours,
    pto.absence_status,
    pto.employee_day_length
  FROM team_status
  INNER JOIN pto ON team_status.employee_id = pto.hr_employee_id
    AND NOT (
      team_status.position_valid_to <= absence_date
      OR team_status.position_valid_from >= absence_date
    )
),

final AS (

  SELECT *

  FROM combined_sources

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-05-15',
    updated_date='2024-07-10',
) }}
