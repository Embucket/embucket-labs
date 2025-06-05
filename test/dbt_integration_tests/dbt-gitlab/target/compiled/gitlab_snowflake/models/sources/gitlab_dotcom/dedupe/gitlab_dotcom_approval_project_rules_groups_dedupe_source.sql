

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_approval_project_rules_groups

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1