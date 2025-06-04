{{ config(
    tags=["mnpi_exception"]
) }}

SELECT *
FROM {{ ref('prep_sales_funnel_kpi') }}
