


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_feature_usages

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY _uploaded_at DESC) = 1