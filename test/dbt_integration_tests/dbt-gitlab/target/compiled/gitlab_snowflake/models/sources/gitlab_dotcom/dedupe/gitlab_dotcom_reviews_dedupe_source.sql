

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_reviews

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1