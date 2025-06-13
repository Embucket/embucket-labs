    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_boards_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER              AS board_id,
    project_id::NUMBER      AS project_id,
    created_at::TIMESTAMP    AS created_at,
    updated_at::TIMESTAMP    AS updated_at,
    milestone_id::NUMBER    AS milestone_id,
    group_id::NUMBER        AS group_id,
    weight::NUMBER          AS weight

  FROM source

)


SELECT *
FROM renamed
