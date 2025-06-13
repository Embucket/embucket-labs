{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH employees as (

    SELECT *
    FROM {{ ref ('employee_directory') }}
    WHERE termination_date IS NULL
      AND hire_date <= CURRENT_DATE()

), contacts AS (

    SELECT *
    FROM {{ ref ('blended_emergency_contacts_source') }}

), contacts_aggregated AS (

    SELECT
      employee_id, 
      SUM(IFF(home_phone IS NOT NULL OR mobile_phone IS NOT NULL OR work_phone IS NOT NULL,1,0)) AS total_emergency_contact_numbers
    FROM contacts
    GROUP BY 1

), final AS (

    SELECT 
      employees.employee_id,
      employees.full_name,
      employees.hire_date,
      employees.last_work_email,
      COALESCE(contacts_aggregated.total_emergency_contact_numbers,0) AS total_emergency_contacts
    FROM employees
    LEFT JOIN contacts_aggregated
      ON employees.employee_id = contacts_aggregated.employee_id

)

SELECT *
FROM final
WHERE total_emergency_contacts = 0
