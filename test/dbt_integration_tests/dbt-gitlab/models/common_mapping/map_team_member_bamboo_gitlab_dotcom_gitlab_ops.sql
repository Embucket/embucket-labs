WITH bamboo_hr_members AS (

    SELECT *
    FROM {{ ref ('bamboohr_work_email') }}
    WHERE work_email IS NOT NULL 
      AND rank_email_desc = 1

), gitlab_dotcom_members AS (

    SELECT * 
    FROM {{ ref ('dim_gitlab_dotcom_gitlab_emails') }}
  
), gitlab_ops_members AS (

    SELECT
      user_id                   AS gitlab_ops_user_id,
      gitlab_ops_user_name, 
      notification_email        AS gitlab_ops_email_address
    FROM {{ ref ('dim_gitlab_ops_gitlab_emails') }}

), missing_employees AS (

    SELECT *
    FROM {{ ref ('sheetload_infrastructure_missing_employees') }}
  
), final AS (

    SELECT 
      bamboo_hr_members.employee_id                     AS bamboohr_employee_id, 
      bamboo_hr_members.full_name                       AS bamboohr_full_name, 
      bamboo_hr_members.work_email                      AS bamboohr_gitlab_email, 
      COALESCE(gitlab_dotcom_members.gitlab_dotcom_user_id, 
               missing_employees.gitlab_dotcom_user_id) AS gitlab_dotcom_user_id,
      gitlab_dotcom_members.gitlab_dotcom_user_name, 
      gitlab_ops_user_id, 
      gitlab_ops_user_name
    FROM bamboo_hr_members
    LEFT JOIN gitlab_dotcom_members
        ON bamboo_hr_members.work_email = gitlab_dotcom_members.gitlab_dotcom_email_address 
    LEFT JOIN gitlab_ops_members
        ON bamboo_hr_members.work_email = gitlab_ops_members.gitlab_ops_email_address 
    LEFT JOIN missing_employees
      ON bamboo_hr_members.employee_id = missing_employees.employee_id

) 

SELECT * 
FROM final
