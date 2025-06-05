

SELECT 
  id::NUMBER AS id,
  tag::VARCHAR AS tag,
  description::VARCHAR AS description,
  project_id::NUMBER AS project_id,
  created_at::TIMESTAMP AS created_at,
  updated_at::TIMESTAMP AS updated_at,
  author_id::NUMBER AS author_id,
  sha::VARCHAR AS sha,
  _uploaded_at::FLOAT AS _uploaded_at
FROM EMBUCKET.tap_postgres.gitlab_db_releases

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1