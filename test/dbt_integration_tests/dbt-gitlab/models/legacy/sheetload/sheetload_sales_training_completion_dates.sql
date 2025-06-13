WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_training_completion_dates_source') }}

)

SELECT *
FROM source