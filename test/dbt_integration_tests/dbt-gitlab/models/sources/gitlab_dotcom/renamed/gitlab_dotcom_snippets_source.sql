    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_snippets_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      id::NUMBER                                     AS snippet_id,
      author_id::NUMBER                              AS author_id,
      project_id::NUMBER                             AS project_id,
      created_at::TIMESTAMP                          AS created_at,
      updated_at::TIMESTAMP                          AS updated_at,
      type::VARCHAR                                  AS snippet_type,
      {{ visibility_level_name('visibility_level') }}   AS visibility_level
      
    FROM source
    
)

SELECT * 
FROM renamed
