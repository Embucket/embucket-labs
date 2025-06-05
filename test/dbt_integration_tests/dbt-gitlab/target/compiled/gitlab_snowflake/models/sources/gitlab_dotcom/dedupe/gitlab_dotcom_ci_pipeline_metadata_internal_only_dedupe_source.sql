

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_pipeline_metadata_internal_only

QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY _uploaded_at DESC) = 1