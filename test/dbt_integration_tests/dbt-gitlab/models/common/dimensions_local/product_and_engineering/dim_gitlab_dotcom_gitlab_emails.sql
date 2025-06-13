{% set column_name = 'notification_email_handle' %}

WITH gitlab_dotcom_members AS (

    SELECT * 
    FROM {{ref('gitlab_dotcom_members')}} 

), gitlab_dotcom_users AS (

    SELECT *,
      SPLIT_PART(notification_email,'@', 0) AS notification_email_handle,
      {{include_gitlab_email(column_name)}} AS include_notification_email
    FROM {{ref('gitlab_dotcom_users')}} 

), sheetload_infrastructure_gitlab_employee AS (

    SELECT * 
    FROM {{ref('sheetload_infrastructure_missing_employees')}}

), gitlab_dotcom_team_members_user_id AS (

    -- This CTE returns the user_id for any team member in the GitLab.com or GitLab.org project 
    SELECT DISTINCT user_id                                    AS gitlab_dotcom_user_id
    FROM gitlab_dotcom_members
    WHERE is_currently_valid = TRUE 
      AND member_source_type = 'Namespace'
      AND source_id IN (9970,6543) -- 9970 = gitlab-org, 6543 = gitlab-com

), notification_email AS (

    -- This CTE cleans and maps GitLab.com user_name and emails for most GitLab team members 
    -- The email field here is notification_email 
    SELECT DISTINCT
      gitlab_dotcom_user_id, 
      user_name,
      CASE
        WHEN length (gitlab_dotcom_users.notification_email) < 3 
          THEN NULL
        WHEN gitlab_dotcom_users.include_notification_email = 'Exclude'
          THEN NULL
        ELSE gitlab_dotcom_users.notification_email                        END AS notification_email
    FROM gitlab_dotcom_team_members_user_id
    INNER JOIN gitlab_dotcom_users
      ON gitlab_dotcom_team_members_user_id.gitlab_dotcom_user_id = gitlab_dotcom_users.user_id
    WHERE user_name NOT ILIKE '%admin%'
  
), all_known_employee_gitlab_emails AS (

    -- This CTE cleans and maps supplemental GitLab.com email addresses from the `emails` table in gitlab_dotcom, and in the case both are null captures work email from sheetload
    SELECT 
      notification_email.gitlab_dotcom_user_id, 
      user_name                                                         AS gitlab_dotcom_user_name,
      COALESCE(notification_email.notification_email,
               sheetload_infrastructure_gitlab_employee.work_email)     AS gitlab_dotcom_email_address
    FROM notification_email
    LEFT JOIN sheetload_infrastructure_gitlab_employee
      ON notification_email.gitlab_dotcom_user_id = sheetload_infrastructure_gitlab_employee.gitlab_dotcom_user_id

) 

SELECT * 
FROM all_known_employee_gitlab_emails

