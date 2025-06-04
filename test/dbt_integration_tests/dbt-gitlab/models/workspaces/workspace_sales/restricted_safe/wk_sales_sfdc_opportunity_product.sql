{{ config(
    tags=["mnpi", "six_hourly"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_product_source') }}

)

SELECT *
FROM source