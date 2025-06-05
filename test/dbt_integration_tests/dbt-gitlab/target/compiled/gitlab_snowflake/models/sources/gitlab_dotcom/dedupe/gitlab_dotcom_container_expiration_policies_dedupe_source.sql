


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_container_expiration_policies

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_at DESC) = 1