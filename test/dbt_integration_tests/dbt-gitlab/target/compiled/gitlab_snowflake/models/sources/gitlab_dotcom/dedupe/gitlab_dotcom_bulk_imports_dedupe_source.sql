


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_bulk_imports

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1