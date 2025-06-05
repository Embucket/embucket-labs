

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_group_audit_events

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1