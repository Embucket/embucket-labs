
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans_source') }}
  
), renamed AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['plan_id'])}} AS dim_plan_sk,
      plan_id AS dim_plan_id,
      --Create calculated field to conform legacy silver and gold plan ids to premium and ultimate plan ids.
      CASE 
        WHEN plan_id = 3 THEN 100
        WHEN plan_id = 4 THEN 101
        ELSE plan_id
      END AS plan_id_modified,
      created_at,
      updated_at,
      plan_name,
      --Create calculated field to conform legacy silver and gold plan names to premium and ultimate plan names.
      CASE 
        WHEN LOWER(plan_name) = 'silver' THEN 'premium'
        WHEN LOWER(plan_name) = 'gold' THEN 'ultimate'
        ELSE plan_name
      END AS plan_name_modified,
      plan_title,
      plan_is_paid

    FROM source

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@iweeks",
    created_date="2021-05-30",
    updated_date="2022-06-09"
) }}
