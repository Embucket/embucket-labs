

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_build_trace_chunks

QUALIFY ROW_NUMBER() OVER (PARTITION BY build_id ORDER BY _uploaded_at DESC) = 1