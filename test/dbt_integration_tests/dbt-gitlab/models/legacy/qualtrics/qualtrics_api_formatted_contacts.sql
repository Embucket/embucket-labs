{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}

), highest_subscription_plan AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_highest_paid_subscription_plan') }}

), renamed AS (

    SELECT
      users.user_id                                                                    AS user_id,
      IFNULL(first_name, SPLIT_PART(users_name, ' ', 1))                               AS first_name,
      LTRIM(IFNULL(last_name,  LTRIM(users_name, SPLIT_PART(users_name, ' ', 1))))     AS last_name,
      IFNULL(notification_email, public_email)                                         AS email_address,
      NULL                                                                             AS phone_number,
      UPPER(IFNULL(NULLIF(preferred_language, 'nan'), 'en'))                           AS language,
      DECODE(highest_paid_subscription_plan_id::VARCHAR, 
        '1',  'Early Adopter',
        '2',  'Bronze',
        '3',  'Silver',
        '4',  'Gold',
        '34', 'Free',
        '67', 'Default', 
        '100', 'Premium', 
        '101', 'Ultimate',
        '102', 'Ultimate Trial',
        '103', 'Premium Trial',
        'Free'
      )                                                                                 AS plan,
      highest_subscription_plan.highest_paid_subscription_namespace_id                  AS namespace_id                    
    FROM users
    LEFT JOIN highest_subscription_plan
      ON users.user_id = highest_subscription_plan.user_id

)

SELECT *
FROM renamed
WHERE email_address IS NOT NULL
