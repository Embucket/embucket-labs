{{ config(
    tags=["mnpi_exception"]
) }}

WITH order_type AS (

    SELECT
      dim_order_type_id,
      order_type_name,
      order_type_grouped
    FROM {{ ref('prep_order_type') }}
)

{{ dbt_audit(
    cte_ref="order_type",
    created_by="@paul_armstrong",
    updated_by="@jpeguero",
    created_date="2020-11-02",
    updated_date="2021-03-23"
) }}
