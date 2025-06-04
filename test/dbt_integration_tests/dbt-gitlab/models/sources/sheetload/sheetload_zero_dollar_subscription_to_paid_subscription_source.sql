WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'zero_dollar_subscription_to_paid_subscription') }}

)

SELECT *
FROM source