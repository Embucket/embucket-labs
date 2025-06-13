WITH source AS (

    SELECT *
    FROM {{ ref ('employee_directory_intermediate') }}

), intermediate AS (


    SELECT DISTINCT
    cost_center,
    division,
    department,
    COUNT(employee_id) AS total_employees
    FROM source
    WHERE date_actual = CURRENT_DATE()
      AND is_termination_date = False 
    GROUP BY 1,2,3

)

SELECT *
FROM intermediate
QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY total_employees DESC) =1
