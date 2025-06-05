

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_namespace_root_storage_statistics

QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) = 1