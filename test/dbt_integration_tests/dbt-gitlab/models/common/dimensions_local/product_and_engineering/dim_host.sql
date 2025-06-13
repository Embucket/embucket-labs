WITH prep_host AS (

    SELECT 
      dim_host_id,
      host_name
    FROM {{ ref('prep_host') }}

)

{{ dbt_audit(
    cte_ref="prep_host",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-03",
    updated_date="2022-03-03"
) }}
