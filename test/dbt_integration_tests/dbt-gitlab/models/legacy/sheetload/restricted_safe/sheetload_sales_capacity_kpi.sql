WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_capacity_kpi_source') }}

)

SELECT *
FROM source

