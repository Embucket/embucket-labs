WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_label_priorities_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                            AS label_priority_id,
      project_id::NUMBER                    AS project_id,
      label_id::NUMBER                      AS label_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT *
FROM renamed
