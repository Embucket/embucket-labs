WITH bamboohr_employment_status AS (

    SELECT *
    FROM {{ ref ('blended_employment_status_source') }}

), employment_log AS (

   SELECT
    employment_status_sequence,
    employee_id,
    employment_status,
    termination_type,
    effective_date                                                                              AS valid_from_date,
    LEAD(effective_date) OVER (PARTITION BY employee_id ORDER BY effective_date, employment_status_sequence)     AS valid_to_date,
    LEAD(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date, employment_status_sequence)  AS next_employment_status,
    LAG(employment_status) OVER (PARTITION BY employee_id ORDER BY effective_date, employment_status_sequence)   AS previous_employment_status
    FROM bamboohr_employment_status

), final AS (

    SELECT
      employee_id,
      employment_status,
      termination_type,
      CASE WHEN previous_employment_status ='Terminated'
        AND employment_status !='Terminated' THEN 'True' ELSE 'False' END                   AS is_rehire,
      next_employment_status,
      valid_from_date                                                                       AS valid_from_date,
      IFF(employment_status='Terminated'
            ,valid_from_date
            ,COALESCE(DATEADD('day',-1,valid_to_date), {{max_date_in_bamboo_analyses()}}))   AS valid_to_date
     FROM employment_log
)

SELECT *
FROM final
