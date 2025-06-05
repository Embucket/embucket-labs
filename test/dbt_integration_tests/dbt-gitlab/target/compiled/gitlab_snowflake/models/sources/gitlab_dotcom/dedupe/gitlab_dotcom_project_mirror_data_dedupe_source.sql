

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_mirror_data

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1