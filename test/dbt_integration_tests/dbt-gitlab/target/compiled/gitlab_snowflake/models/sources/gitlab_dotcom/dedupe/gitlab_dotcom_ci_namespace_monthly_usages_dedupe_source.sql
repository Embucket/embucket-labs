

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_namespace_monthly_usages

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1