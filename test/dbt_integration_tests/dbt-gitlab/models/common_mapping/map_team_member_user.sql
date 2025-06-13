WITH employees AS (
  -- Attempts to get the most recent username for a team member
  SELECT DISTINCT
    employee_id,
    LAST_VALUE(NULLIF(gitlab_username, ''))
    OVER (PARTITION BY employee_id ORDER BY uploaded_at ASC) AS gitlab_username
  FROM {{ ref('blended_employee_mapping_source') }}
  WHERE NULLIF(gitlab_username, '') IS NOT NULL

),

emails AS (
  -- Attempts to get the most recent user email for a team_member
  SELECT DISTINCT
    employee_id,
    LAST_VALUE(work_email) IGNORE NULLS
    OVER (PARTITION BY employee_id ORDER BY uploaded_at ASC) AS gitlab_email
  FROM {{ ref('blended_directory_source') }}
),

users AS (
  -- Attempts to get the most recent user id for a username
  SELECT
    user_id,
    user_name,
    notification_email,
    last_activity_on
  FROM {{ ref('gitlab_dotcom_users_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY last_activity_on DESC ) = 1

),

map AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['employees.employee_id']) }} AS dim_team_member_sk,
    {{ dbt_utils.generate_surrogate_key(['users.user_id']) }} AS dim_user_sk,
    employees.employee_id,
    users.user_id,
    users.user_name AS gitlab_username,
    users.notification_email,
    users.last_activity_on,
    SPLIT_PART(users.notification_email,'@',2) AS email_domain,
    IFF(email_domain = 'gitlab.com',TRUE,FALSE) AS is_gitlab_email,
    ROW_NUMBER() OVER (PARTITION BY employees.employee_id
    ORDER BY is_gitlab_email,last_activity_on) AS preference
  FROM employees
  LEFT JOIN emails
    ON employees.employee_id = emails.employee_id
  LEFT JOIN users
    ON employees.gitlab_username = users.user_name
      OR emails.gitlab_email = users.notification_email
  WHERE users.user_id IS NOT NULL
  AND users.last_activity_on IS NOT NULL
  QUALIFY preference = 1
),

map_clean AS (
  SELECT
    dim_team_member_sk,
    dim_user_sk,
    employee_id,
    user_id,
    gitlab_username,
    notification_email
  FROM map
)

{{ dbt_audit(
    cte_ref="map_clean",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2022-07-22",
    updated_date="2022-08-08"
) }}
