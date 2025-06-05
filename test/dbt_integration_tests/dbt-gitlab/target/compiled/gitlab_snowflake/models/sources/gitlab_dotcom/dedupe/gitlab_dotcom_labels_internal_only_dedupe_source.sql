

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_labels_internal_only

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1