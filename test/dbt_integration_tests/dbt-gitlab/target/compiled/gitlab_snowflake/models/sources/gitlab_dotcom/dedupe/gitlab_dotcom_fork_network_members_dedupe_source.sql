

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_fork_network_members

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1