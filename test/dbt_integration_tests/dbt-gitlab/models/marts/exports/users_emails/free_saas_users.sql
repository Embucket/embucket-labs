WITH users AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'users') }}
    
), memberships AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_memberships') }}
  
), plans AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

), all_gitlab_user_information AS (
  SELECT
        id                                                               AS user_id,
        TRIM(name)                                                       AS full_name,
        SPLIT_PART(TRIM(name), ' ', 1)                                   AS first_name,
        ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(TRIM(name), ' '), 1, 10), ' ') AS last_name,
        username,
        notification_email, 
        state 
    FROM users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), saas_free_users AS (
  
    SELECT DISTINCT
      all_gitlab_user_information.user_id,
      all_gitlab_user_information.full_name,
      all_gitlab_user_information.first_name,
      all_gitlab_user_information.last_name,
      all_gitlab_user_information.notification_email,
      DECODE(
        memberships.ultimate_parent_plan_id,
        '34',    'Free',
        'trial', 'Free Trial',
        'Free'
      ) AS plan_title,
      all_gitlab_user_information.state
    FROM all_gitlab_user_information
    LEFT JOIN memberships ON all_gitlab_user_information.user_id = memberships.user_id
    WHERE memberships.ultimate_parent_plan_id::VARCHAR NOT IN ('2', '3', '4')

)

SELECT *
FROM saas_free_users
