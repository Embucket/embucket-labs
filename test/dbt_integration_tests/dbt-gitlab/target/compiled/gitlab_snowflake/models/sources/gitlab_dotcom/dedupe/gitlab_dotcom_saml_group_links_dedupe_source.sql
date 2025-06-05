

SELECT *
FROM EMBUCKET.tap_postgres.gitlab_db_saml_group_links

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1