{{ config(
    tags=["mnpi_exception"]
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('prep_sfdc_account') }}
    WHERE dim_parent_sales_segment_name_source IS NOT NULL
    
), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.generate_surrogate_key(['dim_parent_sales_segment_name_source']) }}   AS dim_sales_segment_id,
      dim_parent_sales_segment_name_source                                      AS sales_segment_name,
      dim_parent_sales_segment_grouped_source                                   AS sales_segment_grouped
    FROM source_data
    
    UNION ALL

    SELECT
      MD5('-1')                                                                 AS dim_sales_segment_id,
      'Missing sales_segment_name'                                              AS sales_segment_name,
      'Missing sales_segment_grouped'                                           AS sales_segment_grouped

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@lisvinueza",
    created_date="2020-12-18",
    updated_date="2023-05-21"
) }}
