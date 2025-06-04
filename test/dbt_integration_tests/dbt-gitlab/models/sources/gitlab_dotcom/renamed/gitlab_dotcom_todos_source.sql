    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_todos_dedupe_source') }}
    
), renamed AS (
  
    SELECT
      id::NUMBER                                AS todo_id,
      user_id::NUMBER                           AS user_id,
      project_id::NUMBER                        AS project_id,
      target_id::NUMBER                         AS target_id,
      target_type::VARCHAR                      AS target_type,
      author_id::NUMBER                         AS author_id,
      action::NUMBER                            AS todo_action_id,
      state::VARCHAR                            AS todo_state,
      created_at::TIMESTAMP                     AS created_at,
      updated_at::TIMESTAMP                     AS updated_at,
      note_id::NUMBER                           AS note_id,
      commit_id::VARCHAR                        AS commit_id,
      group_id::NUMBER                          AS group_id,
      resolved_by_action::NUMBER                AS resolved_by_action  
      
    FROM source
    
)

SELECT * 
FROM renamed
