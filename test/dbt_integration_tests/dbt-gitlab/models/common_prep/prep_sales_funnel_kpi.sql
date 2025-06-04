{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_sales_funnel_target AS (

    SELECT kpi_name
    FROM {{ ref('prep_sales_funnel_target') }}

), additional_kpi AS (

    SELECT 'Trials' AS kpi_name -- this is the one kpi_name not contained within the targets sheet so we manually include it here.

    UNION -- the rest will be removed once partner targets have been integrated into the sales targets lineage

    SELECT 'Partner Focus Net ARR Company'

    UNION

    SELECT 'Partner Target Net ARR Company'
    
    UNION
    
    SELECT 'Partner Target Net ARR'

    UNION
    
    SELECT 'Partner Focus Net ARR'

), unioned AS (

    SELECT kpi_name
    FROM prep_sales_funnel_target

    UNION

    SELECT kpi_name
    FROM additional_kpi

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['kpi_name'])}} AS dim_sales_funnel_kpi_sk,
  kpi_name                                   AS sales_funnel_kpi_name
FROM unioned
