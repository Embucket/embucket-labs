{{ config(
    tags=["mnpi_exception"]
) }}

WITH alliance_type AS (

    SELECT
      dim_alliance_type_id,
      alliance_type_name,
      alliance_type_short_name,
      valid_from,
      valid_to,
      is_currently_valid
    FROM {{ ref('prep_alliance_type_scd') }}
)

{{ dbt_audit(
    cte_ref="alliance_type",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2021-04-07",
    updated_date="2022-07-18"
) }}
