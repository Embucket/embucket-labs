

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_user_custom_attributes

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1