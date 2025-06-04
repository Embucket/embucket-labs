{{ config(
    tags=["mnpi_exception"]
) }}

WITH source_data AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE sales_qualified_source IS NOT NULL
      AND is_live

), unioned AS (

    SELECT DISTINCT
      MD5(CAST(COALESCE(CAST(sales_qualified_source AS varchar), '') AS varchar))  AS dim_sales_qualified_source_id,
      sales_qualified_source                                                       AS sales_qualified_source_name,
      sales_qualified_source_grouped                                               AS sales_qualified_source_grouped,
      sqs_bucket_engagement
    FROM source_data

    UNION ALL

    SELECT
      MD5('-1')                                                                    AS dim_sales_qualified_source_id,
      'Missing sales_qualified_source_name'                                        AS sales_qualified_source_name,
      'Web Direct Generated'                                                       AS sales_qualified_source_grouped,
      'Co-sell'                                                                    AS sqs_bucket_engagement

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-10-26",
    updated_date="2021-09-09"
) }}
