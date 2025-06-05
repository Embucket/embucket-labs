

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_protected_branch_push_access_levels

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1