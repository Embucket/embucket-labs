WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_mirror_data_dedupe_source') }}
  

), renamed AS (

    SELECT

      id::NUMBER                                     AS project_mirror_data_id,
      project_id::NUMBER                             AS project_id,
      retry_count::NUMBER                            AS retry_count,
      last_update_started_at::TIMESTAMP               AS last_update_started_at,
      last_update_scheduled_at::TIMESTAMP             AS last_update_scheduled_at,
      next_execution_timestamp::TIMESTAMP             AS next_execution_timestamp

    FROM source

)

SELECT *
FROM renamed
