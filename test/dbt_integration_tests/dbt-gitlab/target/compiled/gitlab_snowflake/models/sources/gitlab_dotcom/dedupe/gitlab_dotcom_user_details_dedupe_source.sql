

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_user_details

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _uploaded_at DESC) = 1