






SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_work_item_types

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1