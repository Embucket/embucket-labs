WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_resource_weight_events_dedupe_source') }}
  
), renamed AS (

    SELECT
      id                                             AS resource_weight_event_id,
      user_id::NUMBER                               AS user_id,
      issue_id::NUMBER                              AS issue_id,
      weight::NUMBER                                AS weight,
      created_at::TIMESTAMP                          AS created_at
    FROM source

)

SELECT *
FROM renamed
