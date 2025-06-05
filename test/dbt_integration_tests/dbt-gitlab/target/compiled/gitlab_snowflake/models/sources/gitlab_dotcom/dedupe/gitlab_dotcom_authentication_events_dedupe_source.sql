


SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_authentication_events

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1