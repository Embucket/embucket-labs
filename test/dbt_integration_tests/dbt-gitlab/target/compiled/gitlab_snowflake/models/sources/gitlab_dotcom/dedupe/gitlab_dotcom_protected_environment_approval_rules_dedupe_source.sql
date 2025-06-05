
 
SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_protected_environment_approval_rules

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1