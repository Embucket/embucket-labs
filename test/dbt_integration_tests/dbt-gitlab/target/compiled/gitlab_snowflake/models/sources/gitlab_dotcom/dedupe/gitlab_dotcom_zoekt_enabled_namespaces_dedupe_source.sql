


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_zoekt_enabled_namespaces

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1