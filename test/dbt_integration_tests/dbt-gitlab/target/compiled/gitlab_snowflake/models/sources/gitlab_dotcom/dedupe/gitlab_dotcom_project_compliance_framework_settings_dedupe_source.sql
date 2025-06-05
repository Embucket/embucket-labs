


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_compliance_framework_settings

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY _uploaded_at DESC) = 1