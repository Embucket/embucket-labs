WITH team_member_status AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC)                                  AS row_number
  FROM {{ref('workday_employment_status_source')}}
  WHERE effective_date <= CURRENT_DATE()
  AND effective_date >= '2022-06-16'
  /* Ensure future terms are not included in this table.
  A future iteration should add row level access policies to show future terms to 
  the analyst_people role only
  */
),

bamboohr_status AS (
  SELECT *
  FROM {{ ref('bamboohr_employment_status_source') }}
  WHERE effective_date < '2022-06-16'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id,effective_date ORDER BY status_id DESC) = 1
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

historic_status AS (
  SELECT
    map.wk_employee_id AS employee_id,
    bamboohr_status.employment_status,
    NULL AS termination_reason,
    bamboohr_status.termination_type,
    NULL AS exit_impact,
    bamboohr_status.effective_date,
    ROW_NUMBER() OVER (PARTITION BY bamboohr_status.employee_id ORDER BY bamboohr_status.effective_date DESC) AS row_number
  FROM bamboohr_status
  INNER JOIN map
    ON bamboohr_status.employee_id = map.bhr_employee_id
 
),

combined_sources AS (
  SELECT 
    employee_id,
    employment_status,
    termination_reason,
    termination_type,
    exit_impact,
    effective_date,
    row_number
  FROM team_member_status

  UNION

  SELECT 
    employee_id,
    employment_status,
    termination_reason,
    termination_type,
    exit_impact,
    effective_date,
    row_number
  FROM historic_status

),

final AS (

  SELECT

    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'employment_status', 'effective_date'])}}    AS team_member_status_pk,
    -- Surrogate key                                                                                                     
    {{ dbt_utils.generate_surrogate_key(['employee_id'])}}                                           AS dim_team_member_sk,

    -- Team member status attributes
    employee_id                                                                             AS employee_id,
    employment_status                                                                       AS employment_status,
    termination_reason                                                                      AS termination_reason,
    termination_type                                                                        AS termination_type,
    exit_impact                                                                             AS exit_impact,
    effective_date                                                                          AS status_effective_date,

    -- Add is_current flag for most current team member record, especially in case of rehires
    IFF(row_number = 1, TRUE, FALSE)                                                        AS is_current
  FROM combined_sources

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@pempey',
    created_date='2023-05-30',
    updated_date='2023-07-10'
) }}
