
WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'devrel_blog_posts_views') }}
), renamed AS (
  SELECT 
    publication_date::VARCHAR            as publication_date,
    gitlab_publication_quarter::VARCHAR  as gitlab_publication_quarter,
    blog_title::VARCHAR                  as blog_title,
    url::VARCHAR     as url,
    author::VARCHAR  as author,
    team::VARCHAR    as team

  FROM source
)
SELECT *
FROM renamed
WHERE url IS NOT NULL