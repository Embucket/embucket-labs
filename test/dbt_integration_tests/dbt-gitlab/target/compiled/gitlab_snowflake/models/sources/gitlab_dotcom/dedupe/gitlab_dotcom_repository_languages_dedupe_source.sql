

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_repository_languages

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_programming_language_id ORDER BY _uploaded_at DESC) = 1