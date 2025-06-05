


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_users_ops_dashboard_projects

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1