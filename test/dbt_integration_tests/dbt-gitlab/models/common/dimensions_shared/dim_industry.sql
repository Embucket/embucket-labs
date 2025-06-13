{{ config(
    tags=["mnpi_exception"]
) }}

WITH industry AS (

    SELECT
      dim_industry_id,
      industry_name
    FROM {{ ref('prep_industry') }}

)

{{ dbt_audit(
    cte_ref="industry",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-10-26",
    updated_date="2020-12-18"
) }}
