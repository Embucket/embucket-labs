

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_incident_management_issuable_escalation_statuses

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1