WITH
{{ distinct_source(source=source('gitlab_dotcom', 'gitlab_subscriptions'))}}

, renamed AS (

    SELECT DISTINCT

      id::NUMBER                                   AS gitlab_subscription_id,
      start_date::DATE                             AS gitlab_subscription_start_date,
      end_date::DATE                               AS gitlab_subscription_end_date,
      trial_starts_on::DATE                        AS gitlab_subscription_trial_starts_on,
      trial_ends_on::DATE                          AS gitlab_subscription_trial_ends_on,
      namespace_id::NUMBER                         AS namespace_id,
      hosted_plan_id::NUMBER                       AS plan_id,
      max_seats_used::NUMBER                       AS max_seats_used,
      seats::NUMBER                                AS seats,
      true::BOOLEAN                               AS is_trial,
      seats_in_use::NUMBER                         AS seats_in_use,
      seats_owed::NUMBER                           AS seats_owed,
      trial_extension_type::NUMBER                 AS trial_extension_type,
      created_at::TIMESTAMP                        AS created_at,
      updated_at::TIMESTAMP                        AS updated_at,
      valid_from -- Column was added in distinct_source CTE
  
    FROM distinct_source
    WHERE gitlab_subscription_id != 572635 -- This ID has NULL values for many of the important columns. 
      AND namespace_id IS NOT NULL 

)

/* Note: the primary key used is namespace_id, not subscription id.
   This matches our business use case better. */
{{ scd_type_2(
    primary_key_renamed='namespace_id', 
    primary_key_raw='namespace_id'
) }}
