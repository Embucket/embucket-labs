WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_multiple_delivery_types_per_month_charge_ids_source') }}

)

SELECT *
FROM source
