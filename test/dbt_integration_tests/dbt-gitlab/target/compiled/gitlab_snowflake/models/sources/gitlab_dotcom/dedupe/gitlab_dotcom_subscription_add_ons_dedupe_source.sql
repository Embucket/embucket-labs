

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_subscription_add_ons

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1