WITH bamboohr_employee_id_mapping_source AS (
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping_source') }}
  WHERE uploaded_row_number_desc = 1 --excludes team members deleted from BambooHR
),

staffing_history_approved_source AS (
  SELECT *
  FROM {{ ref('staffing_history_approved_source') }}
),

bamboohr_status AS (
  SELECT *
  FROM {{ ref('bamboohr_employment_status_source') }}
)
,
bamboohr_employee_mapping AS (
  SELECT DISTINCT
    employee_id     AS bhr_employee_id,
    employee_number AS wk_employee_id
  FROM bamboohr_employee_id_mapping_source
),

bamboohr_terminations AS (
  SELECT DISTINCT
    employee_number AS wk_employee_id,
    hire_date,
    termination_date
  FROM bamboohr_employee_id_mapping_source
  WHERE termination_date <= '2020-12-31' --use Workday data for 2021-01-01 or later
    AND hire_date <= '2020-12-31'      --use Workday data for 2021-01-01 or later
    AND wk_employee_id != '11595'      --incorrect hire date in bhr_map, listed as 2020-09-10. Correct date is 2020-06-09
    OR wk_employee_id = '11202'        --not in Workday and terminated on 2021-10-29 so WHERE clause would have excluded this team member
),


bamboohr_termination_status AS (
  SELECT
    bamboohr_employee_mapping.wk_employee_id,
    bamboohr_status.effective_date,
    bamboohr_status.employment_status
  FROM bamboohr_status
  LEFT JOIN bamboohr_employee_mapping ON bamboohr_status.employee_id = bamboohr_employee_mapping.bhr_employee_id
  WHERE bamboohr_status.employment_status = 'Terminated'
    AND bamboohr_status.effective_date <= '2020-12-31'
),

bamboohr_rehires AS (
  SELECT
    *,
    IFF(employment_status = 'Terminated', 1, 0) AS is_terminated,
    LEAD(employment_status) OVER (
      PARTITION BY
        employee_id ORDER BY effective_date DESC,
      is_terminated DESC
    )                                           AS prior_reason
  FROM bamboohr_status
  WHERE effective_date <= '2020-12-31'
  QUALIFY
    prior_reason = 'Terminated'
    AND employment_status != 'Terminated'
  ORDER BY
    employee_id ASC,
    effective_date DESC
),

bamboohr_hires1 AS (-- displays first non-terminated record for any team members in bamboohr_rehires CTE to capture original hire dateAS (
  SELECT bamboohr_status.*
  FROM bamboohr_status
  INNER JOIN bamboohr_rehires ON bamboohr_status.employee_id = bamboohr_rehires.employee_id
  WHERE bamboohr_status.effective_date <= '2020-12-31'
    AND bamboohr_status.employment_status != 'Terminated' QUALIFY ROW_NUMBER() OVER (
    PARTITION BY bamboohr_status.employee_id ORDER BY bamboohr_status.effective_date ASC
  ) = 1
),

bamboohr_hires2 AS ( --displays non-terminated record for team members not in bamboohr_rehires CTEAS (
  SELECT bamboohr_status.*
  FROM bamboohr_status
  LEFT JOIN bamboohr_rehires ON bamboohr_status.employee_id = bamboohr_rehires.employee_id
  WHERE bamboohr_status.effective_date <= '2020-12-31'
    AND bamboohr_status.employment_status != 'Terminated'
    AND bamboohr_rehires.employee_id IS NULL
    AND bamboohr_status.status_id NOT IN (
      '32108',
      '27971',
      '29556'
    ) -- no hire date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY bamboohr_status.employee_id ORDER BY bamboohr_status.effective_date ASC
  ) = 1
),

bamboohr_hires AS (
  SELECT
    employee_id,
    effective_date
  FROM bamboohr_hires1

  UNION

  SELECT
    employee_id,
    effective_date
  FROM bamboohr_rehires

  UNION

  SELECT
    employee_id,
    effective_date
  FROM bamboohr_hires2
),

hires_stage AS (
  SELECT
    bamboohr_employee_mapping.wk_employee_id,
    bamboohr_hires.effective_date
  FROM bamboohr_hires
  LEFT JOIN bamboohr_employee_mapping ON bamboohr_hires.employee_id = bamboohr_employee_mapping.bhr_employee_id

  UNION

  SELECT
    employee_id    AS hire_id,
    effective_date AS hire_date
  FROM staffing_history_approved_source
  WHERE business_process_type IN (
      'Hire',
      'Contract Contingent Worker'
    )

  UNION

  SELECT
    wk_employee_id,
    hire_date
  FROM bamboohr_terminations
),

terminations_stage AS (
  SELECT
    wk_employee_id,
    termination_date
  FROM bamboohr_terminations

  UNION

  SELECT
    employee_id,
    effective_date
  FROM staffing_history_approved_source
  WHERE business_process_type IN (
      'Termination',
      'End Contingent Worker Contract'
    )


  UNION

  SELECT
    wk_employee_id,
    effective_date
  FROM bamboohr_termination_status
  WHERE employment_status = 'Terminated'
),

start_date AS (
  SELECT
    wk_employee_id AS hire_id,
    effective_date AS hire_date,
    ROW_NUMBER() OVER (
      PARTITION BY wk_employee_id ORDER BY effective_date ASC
    )              AS hire_rank_asc
  FROM hires_stage
),

end_date AS (
  SELECT
    wk_employee_id   AS term_id,
    termination_date AS term_date,
    ROW_NUMBER() OVER (
      PARTITION BY wk_employee_id ORDER BY termination_date ASC
    )                AS term_rank_asc
  FROM terminations_stage
),

combined AS (
  SELECT
    -- Surrogate keys
    {{ dbt_utils.generate_surrogate_key(['start_date.hire_id']) }} AS dim_team_member_sk,
    -- Team member history attributes
    start_date.hire_id                                                                              AS employee_id,
    start_date.hire_rank_asc,
    start_date.hire_date                                                                            AS hired_date,
    end_date.term_date                                                                              AS term_date,
    end_date.term_rank_asc,
    COALESCE(end_date.term_date, CURRENT_DATE())                                                    AS last_date
  FROM start_date
  LEFT JOIN end_date ON start_date.hire_id = end_date.term_id
    AND start_date.hire_rank_asc = end_date.term_rank_asc
),

final AS (
  SELECT
    dim_team_member_sk,
    employee_id,
    hire_rank_asc,
    CASE
      WHEN employee_id = '11595'
        AND hire_rank_asc = 1
        THEN '2020-09-10'
      WHEN employee_id = '10718'
        AND hire_rank_asc = 2
        THEN '2021-02-15'
      ELSE hired_date
    END AS hire_date,
    term_date,
    term_rank_asc,
    last_date
  FROM combined
  ORDER BY
    employee_id ASC,
    hire_rank_asc DESC
)

SELECT *
FROM final
