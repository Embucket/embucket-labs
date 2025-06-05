

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_pipeline_schedule_variables

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1