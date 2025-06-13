    
WITH source AS (

      SELECT *
      FROM {{ ref('gitlab_dotcom_lists_dedupe_source') }}
      
), renamed AS (

      SELECT
        id::NUMBER               AS list_id,
        board_id::NUMBER         AS board_id,
        label_id::NUMBER         AS label_id,
        list_type::NUMBER        AS list_type,
        created_at::TIMESTAMP     AS created_at,
        updated_at::TIMESTAMP     AS updated_at,
        user_id::NUMBER          AS user_id,
        milestone_id::NUMBER     AS milestone_id,
        max_issue_count::NUMBER  AS max_issue_count,
        max_issue_weight::NUMBER AS max_issue_weight,
        limit_metric::VARCHAR     AS limit_metric
      FROM source

)

SELECT *
FROM renamed
