WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_statistics_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                      AS namespace_statistics_id,
      namespace_id::NUMBER                            AS namespace_id,
      shared_runners_seconds::NUMBER                  AS shared_runners_seconds,
      shared_runners_seconds_last_reset::TIMESTAMP     AS shared_runners_seconds_last_reset

    FROM source

)

SELECT *
FROM renamed
