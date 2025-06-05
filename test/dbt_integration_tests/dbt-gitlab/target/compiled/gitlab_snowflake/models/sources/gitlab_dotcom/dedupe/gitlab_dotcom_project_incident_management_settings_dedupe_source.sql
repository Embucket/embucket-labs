


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_project_incident_management_settings

QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY _uploaded_at DESC) = 1