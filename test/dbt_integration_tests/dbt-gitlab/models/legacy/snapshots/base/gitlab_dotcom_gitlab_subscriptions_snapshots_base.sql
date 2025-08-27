{{ config({
    "alias": "gitlab_dotcom_gitlab_subscriptions_snapshots"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'gitlab_dotcom_gitlab_subscriptions_snapshots') }}
  WHERE id != 572635 -- This ID has NULL values for many of the important columns.
    AND namespace_id IS NOT NULL

), renamed AS (

  SELECT
    dbt_scd_id::VARCHAR                           AS gitlab_subscription_snapshot_id,
    id::NUMBER                                    AS gitlab_subscription_id,
    start_date::DATE                              AS gitlab_subscription_start_date,
    end_date::DATE                                AS gitlab_subscription_end_date,
    trial_ends_on::DATE                           AS gitlab_subscription_trial_ends_on,
    namespace_id::NUMBER                          AS namespace_id,
    hosted_plan_id::NUMBER                        AS plan_id,
    max_seats_used::NUMBER                        AS max_seats_used,
    seats::NUMBER                                 AS seats,
    trial::BOOLEAN                                AS is_trial,
    created_at::TIMESTAMP                         AS gitlab_subscription_created_at,
    updated_at::TIMESTAMP                         AS gitlab_subscription_updated_at,
    dbt_valid_from::TIMESTAMP                   AS valid_from,
    dbt_valid_to::TIMESTAMP                     AS valid_to
  
  FROM source
    
)

SELECT *
FROM renamed
