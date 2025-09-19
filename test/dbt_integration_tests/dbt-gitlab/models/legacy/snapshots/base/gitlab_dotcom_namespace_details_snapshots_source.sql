WITH snapshots AS (

  SELECT *
  FROM {{ source('snapshots', 'gitlab_dotcom_namespace_details_snapshots') }}
    
), renamed as (

  SELECT
    dbt_scd_id::VARCHAR                                           AS namespace_details_snapshot_id,
    namespace_id::NUMBER                                          AS namespace_id,
    free_user_cap_over_limit_notified_at::TIMESTAMP               AS free_user_cap_over_limit_notified_at,
    dashboard_notification_at::TIMESTAMP                          AS dashboard_notification_at,
    dashboard_enforcement_at::TIMESTAMP                           AS dashboard_enforcement_at,
    created_at::TIMESTAMP                                         AS created_at,
    updated_at::TIMESTAMP                                         AS updated_at,
    
    dbt_valid_from::TIMESTAMP                                   AS valid_from,
    dbt_valid_to::TIMESTAMP                                     AS valid_to
  FROM snapshots
    
)

SELECT *
FROM renamed
