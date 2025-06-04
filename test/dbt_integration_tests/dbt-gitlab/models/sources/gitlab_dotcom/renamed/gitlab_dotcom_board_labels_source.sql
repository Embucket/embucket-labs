WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_board_labels_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER        AS board_label_relation_id,
    board_id::NUMBER  AS board_id,
    label_id::NUMBER  AS label_id

  FROM source

)


SELECT *
FROM renamed
