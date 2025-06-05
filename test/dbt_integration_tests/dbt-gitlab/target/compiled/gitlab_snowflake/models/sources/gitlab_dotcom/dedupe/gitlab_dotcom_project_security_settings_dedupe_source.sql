






SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_security_settings

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_at DESC) = 1