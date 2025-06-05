

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_programming_languages

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1