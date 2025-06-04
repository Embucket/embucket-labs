{{ config(
    tags=["mnpi_exception"]
) }}

WITH partner_category AS (

    SELECT
      dim_partner_category_id,
      partner_category_name
    FROM {{ ref('prep_partner_category') }}
)

{{ dbt_audit(
    cte_ref="partner_category",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-03-10",
    updated_date="2023-03-10"
) }}
