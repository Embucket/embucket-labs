WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_analytics_tableau_rls_users_source') }}

)

SELECT *
FROM source