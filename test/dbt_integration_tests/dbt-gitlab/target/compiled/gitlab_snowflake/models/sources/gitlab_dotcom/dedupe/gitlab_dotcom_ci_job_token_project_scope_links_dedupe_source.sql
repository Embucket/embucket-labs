

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_job_token_project_scope_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1