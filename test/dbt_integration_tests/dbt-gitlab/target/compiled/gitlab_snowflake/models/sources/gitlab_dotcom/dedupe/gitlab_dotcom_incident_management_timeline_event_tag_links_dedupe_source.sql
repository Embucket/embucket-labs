

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_incident_management_timeline_event_tag_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1