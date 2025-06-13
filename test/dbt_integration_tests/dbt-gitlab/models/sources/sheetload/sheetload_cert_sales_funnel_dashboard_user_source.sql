WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_sales_funnel_dashboard_user') }}

)

SELECT *
FROM source


