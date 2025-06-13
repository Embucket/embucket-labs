WITH source AS (
  SELECT *
  FROM {{ ref('prep_gitlab_dotcom_plan') }}
),

rename AS (
  SELECT
    dim_plan_sk,
    dim_plan_id,
    plan_id_modified,
    plan_name,
    plan_name_modified,
    plan_title,
    plan_is_paid AS is_plan_paid
  FROM source
),

missing_member AS (
  SELECT
    MD5('-1') AS dim_plan_sk,
    -1        AS dim_plan_id,
    -1        AS plan_id_modified,
    'Unknown' AS plan_name,
    'Unknown' AS plan_name_modified,
    'Unknown' AS plan_title,
    FALSE     AS is_plan_paid

),

final AS (
  SELECT *
  FROM rename

  UNION ALL

  SELECT *
  FROM missing_member
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2023-04-25",
    updated_date="2023-04-25"
) }}
