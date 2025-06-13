WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'multiple_delivery_types_per_month_charge_ids') }}

)

SELECT *
FROM source
