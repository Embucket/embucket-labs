
WITH workday AS (

    SELECT *
    FROM {{ ref('workday_employment_status_source') }}
),

final AS (
    
    SELECT
      employee_id,
      uploaded_at,
      effective_date,
      employment_status,
      exit_impact,
      termination_reason,
      initiated_at
    FROM workday
    WHERE employment_status = 'Terminated'
)

SELECT *
FROM final
