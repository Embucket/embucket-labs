WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_location_factor_targets_source') }}

)

SELECT *
FROM source