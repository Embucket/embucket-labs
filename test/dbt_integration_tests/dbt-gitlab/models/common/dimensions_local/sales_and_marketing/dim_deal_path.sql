{{ config(
    tags=["mnpi_exception"]
) }}


{{config({
    "materialized": "table",
  })
}}

WITH deal_path AS (

    SELECT
      dim_deal_path_id,
      deal_path_name
    FROM {{ ref('prep_deal_path' )}}
)

{{ dbt_audit(
    cte_ref="deal_path",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2021-02-26"
) }}
