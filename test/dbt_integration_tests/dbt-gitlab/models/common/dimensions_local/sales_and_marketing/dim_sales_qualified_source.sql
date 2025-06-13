{{ config(
    tags=["mnpi_exception"]
) }}

WITH sales_qualified_source AS (

    SELECT
      dim_sales_qualified_source_id,
      sales_qualified_source_name,
      sales_qualified_source_grouped,
      sqs_bucket_engagement
    FROM {{ ref('prep_sales_qualified_source') }}

)

{{ dbt_audit(
    cte_ref="sales_qualified_source",
    created_by="@paul_armstrong",
    updated_by="@jpeguero",
    created_date="2020-10-26",
    updated_date="2021-09-09"
) }}
