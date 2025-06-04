WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_repository_languages_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      MD5(project_programming_language_id)::VARCHAR AS repository_language_id,
      project_id::NUMBER                           AS project_id,
      programming_language_id::NUMBER              AS programming_language_id,
      share::FLOAT                                  AS share
    FROM source
    
)

SELECT * 
FROM renamed
