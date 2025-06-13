WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_zero_dollar_subscription_to_paid_subscription_source') }}

)

SELECT *
FROM source