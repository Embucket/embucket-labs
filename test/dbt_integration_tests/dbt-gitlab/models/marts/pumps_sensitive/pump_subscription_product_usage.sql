{{ config(
    tags=["product", "mnpi_exception"]
) }}

SELECT *
FROM {{ ref('subscription_product_usage_data') }}
