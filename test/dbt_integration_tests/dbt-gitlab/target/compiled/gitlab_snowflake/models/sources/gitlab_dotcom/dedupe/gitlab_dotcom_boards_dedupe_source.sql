

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_boards

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1