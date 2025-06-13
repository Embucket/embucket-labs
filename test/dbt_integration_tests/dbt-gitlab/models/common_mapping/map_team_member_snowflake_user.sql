WITH snowflake_users AS (
  SELECT *
  FROM {{ ref('snowflake_show_users_source') }}
  QUALIFY MAX(snapshot_date) OVER () = snapshot_date
),

team_member AS (
  SELECT *
  FROM {{ ref('all_workers_source') }}
),

mapping AS (
  SELECT
    team_member.employee_id,
    snowflake_users.user_name AS snowflake_user_name
  FROM team_member
  LEFT JOIN snowflake_users
    ON LOWER(TRIM(team_member.work_email)) = LOWER(TRIM(snowflake_users.login_name))
  WHERE snowflake_users.user_name IS NOT NULL
)

SELECT *
FROM mapping
