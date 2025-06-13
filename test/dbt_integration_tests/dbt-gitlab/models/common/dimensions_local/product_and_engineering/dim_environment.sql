WITH environment AS (

    SELECT
      dim_environment_id,
      environment
    FROM {{ ref('prep_environment') }}

)

{{ dbt_audit(
    cte_ref="environment",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-09-22",
    updated_date="2021-09-22"
) }}
