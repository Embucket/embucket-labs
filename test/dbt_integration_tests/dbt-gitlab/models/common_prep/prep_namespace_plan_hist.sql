{{ simple_cte([('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
               ('dim_date', 'dim_date')
])}}
, source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_gitlab_subscriptions_namespace_id_snapshots') }}
    WHERE id != 572635 -- This ID has NULL values for many of the important columns.

), renamed AS (

    SELECT
      dbt_scd_id::VARCHAR                           AS dim_namespace_plan_subscription_hist_id,
      id::NUMBER                                    AS dim_plan_subscription_id,
      start_date::DATE                              AS plan_subscription_start_date,
      end_date::DATE                                AS plan_subscription_end_date,
      trial_ends_on::DATE                           AS plan_subscription_trial_end_date,
      namespace_id::NUMBER                          AS dim_namespace_id,
      hosted_plan_id::NUMBER                        AS dim_plan_id,
      max_seats_used::NUMBER                        AS max_seats_used,
      seats::NUMBER                                 AS seats,
      trial::BOOLEAN                                AS is_trial,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
      seats_in_use::NUMBER                          AS seats_in_use,
      seats_owed::NUMBER                            AS seats_owed,
      trial_extension_type::NUMBER                  AS trial_extension_type,
      "DBT_VALID_FROM"::TIMESTAMP                   AS valid_from,
      "DBT_VALID_TO"::TIMESTAMP                     AS valid_to 
    FROM source
    
), joined AS (

    SELECT
      -- primary key
      renamed.dim_namespace_plan_subscription_hist_id,

      -- foreign keys
      renamed.dim_plan_subscription_id,
      renamed.dim_namespace_id, 
      --Please see dbt docs for a description of this column transformation.
      CASE 
        WHEN renamed.is_trial = TRUE AND LOWER(prep_gitlab_dotcom_plan.plan_name_modified) = 'ultimate' THEN 102
        WHEN renamed.dim_plan_id IS NULL THEN 34
        ELSE prep_gitlab_dotcom_plan.plan_id_modified
      END AS dim_plan_id,

      -- date dimensions
      plan_subscription_start_date.date_id     AS plan_subscription_start_date_id,
      plan_subscription_end_date.date_id       AS plan_subscription_end_date_id,
      plan_subscription_trial_end_date.date_id AS plan_subscription_trial_end_date_id,

      -- namespace_plan metadata
      renamed.max_seats_used,
      renamed.seats,
      renamed.is_trial,
      renamed.created_at,
      renamed.updated_at,
      renamed.seats_in_use,
      renamed.seats_owed,
      renamed.trial_extension_type,

      -- hist dimensions
      renamed.valid_from,
      renamed.valid_to
    FROM renamed
    LEFT JOIN dim_date AS plan_subscription_start_date
      ON renamed.plan_subscription_start_date = plan_subscription_start_date.date_day
    LEFT JOIN dim_date AS plan_subscription_end_date
      ON renamed.plan_subscription_end_date = plan_subscription_end_date.date_day
    LEFT JOIN dim_date AS plan_subscription_trial_end_date
      ON renamed.plan_subscription_trial_end_date = plan_subscription_trial_end_date.date_day
    LEFT JOIN prep_gitlab_dotcom_plan
      ON renamed.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
    WHERE renamed.dim_namespace_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@iweeks",
    created_date="2021-05-30",
    updated_date="2022-06-09"
) }}
