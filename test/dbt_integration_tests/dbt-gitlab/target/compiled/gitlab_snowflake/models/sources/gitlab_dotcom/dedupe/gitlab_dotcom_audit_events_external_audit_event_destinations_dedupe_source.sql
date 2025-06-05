


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_audit_events_external_audit_event_destinations

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1