{{ config({
    "alias": "gitlab_dotcom_application_settings_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_application_settings_snapshots') }}
    
), renamed as (

  SELECT
    dbt_scd_id::VARCHAR                                           AS application_settings_snapshot_id,
    dbt_valid_from::TIMESTAMP                                     AS valid_from,
    dbt_valid_to::TIMESTAMP                                       AS valid_to,
    id::NUMBER                                                    AS application_settings_id,
    shared_runners_minutes::NUMBER                                AS shared_runners_minutes,
    repository_size_limit::NUMBER                                 AS repository_size_limit
  FROM source
    
)

SELECT *
FROM renamed
