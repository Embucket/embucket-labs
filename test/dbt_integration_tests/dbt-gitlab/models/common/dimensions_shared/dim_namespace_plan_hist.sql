WITH prep_namespace_plan_hist AS (

    SELECT
      dim_namespace_plan_subscription_hist_id,

          -- foreign keys
      dim_plan_subscription_id,
      dim_namespace_id,
      dim_plan_id,

          -- date dimensions
      plan_subscription_start_date_id,
      plan_subscription_end_date_id,
      plan_subscription_trial_end_date_id,

          -- namespace_plan metadata
      max_seats_used,
      seats,
      is_trial,
      created_at,
      updated_at,
      seats_in_use,
      seats_owed,
      trial_extension_type,

          -- hist dimensions
      valid_from,
      valid_to
    FROM {{ ref('prep_namespace_plan_hist') }}
)

{{ dbt_audit(
    cte_ref="prep_namespace_plan_hist",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-30",
    updated_date="2021-05-30"
) }}
