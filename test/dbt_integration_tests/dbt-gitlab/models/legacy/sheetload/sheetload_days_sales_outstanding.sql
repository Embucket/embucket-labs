WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_days_sales_outstanding_source') }}

)

SELECT *
FROM source
