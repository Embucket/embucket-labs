

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_label_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1