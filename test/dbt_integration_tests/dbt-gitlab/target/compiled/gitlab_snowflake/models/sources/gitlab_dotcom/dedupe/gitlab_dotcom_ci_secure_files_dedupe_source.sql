

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_secure_files

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1