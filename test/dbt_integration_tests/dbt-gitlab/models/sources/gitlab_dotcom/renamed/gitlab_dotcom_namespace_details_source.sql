WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_details_dedupe_source') }}

), renamed AS (

    SELECT

      namespace_id::NUMBER                                     AS namespace_id,
      free_user_cap_over_limit_notified_at::TIMESTAMP          AS free_user_cap_over_limit_notified_at,
      dashboard_notification_at::TIMESTAMP                     AS dashboard_notification_at,
      dashboard_enforcement_at::TIMESTAMP                      AS dashboard_enforcement_at,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at

    FROM source

)

SELECT *
FROM renamed
