

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_ci_subscriptions_projects

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _UPLOADED_AT DESC) = 1