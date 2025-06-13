{{ config({
    "alias": "gitlab_dotcom_namespace_statistics_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_namespace_statistics_snapshots') }}
    
), renamed as (

  SELECT
  
    dbt_scd_id::VARCHAR                                           AS namespace_statistics_snapshot_id,
    id::NUMBER                                                   AS namespace_statistics_id,
    namespace_id::NUMBER                                         AS namespace_id,
    shared_runners_seconds::NUMBER                               AS shared_runners_seconds,
    shared_runners_seconds_last_reset::TIMESTAMP                  AS shared_runners_seconds_last_reset,
    dbt_valid_from::TIMESTAMP                                     AS valid_from,
    dbt_valid_to::TIMESTAMP                                       AS valid_to

  FROM source
    
)

SELECT *
FROM renamed
