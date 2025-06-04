{{ config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH hiring_team AS (
    
    SELECT *
    FROM {{ref('greenhouse_hiring_team_source')}}

), greenhouse_users AS (

    SELECT *
    FROM {{ref('greenhouse_users_source')}}

), employees AS (

    SELECT *
    FROM {{ref('employee_directory')}}      

), final AS (

    SELECT
      hiring_team.job_id,
      hiring_team.hiring_team_role,
      hiring_team.is_responsible,
      greenhouse_users.employee_id,
      employees.first_name || ' ' || employees.last_name AS full_name,
      hiring_team.hiring_team_created_at,
      hiring_team.hiring_team_updated_at
    FROM hiring_team
    LEFT JOIN greenhouse_users
      ON hiring_team.user_id = greenhouse_users.user_id
    LEFT JOIN employees
      ON employees.employee_number = greenhouse_users.employee_id  

)

SELECT *
FROM final
