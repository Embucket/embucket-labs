{{ config(alias='wk_sales_tableau_rls_users') }}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_analytics_tableau_rls_users_source') }}

)


SELECT *
FROM source
