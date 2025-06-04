WITH source AS (
  SELECT
    {{ get_date_id('date_actual') }}                                AS date_id,
    *
  FROM {{ ref('date_details_source') }}
)

{{ dbt_audit(
    cte_ref="source",
    created_by="@pempey",
    updated_by="@jpeguero",
    created_date="2023-01-09",
    updated_date="2023-08-14"
) }}
