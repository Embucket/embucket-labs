WITH source AS (

    SELECT *
    FROM {{ source('gitlab_ops', 'merge_request_metrics') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                              AS merge_request_metric_id,
      merge_request_id::NUMBER                                AS merge_request_id,

      latest_build_started_at::TIMESTAMP                       AS latest_build_started_at,
      latest_build_finished_at::TIMESTAMP                      AS latest_build_finished_at,
      first_deployed_to_production_at::TIMESTAMP               AS first_deployed_to_production_at,
      merged_at::TIMESTAMP                                     AS merged_at,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at
      
    FROM source

)

SELECT *
FROM renamed