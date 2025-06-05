






SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_pipl_users

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1