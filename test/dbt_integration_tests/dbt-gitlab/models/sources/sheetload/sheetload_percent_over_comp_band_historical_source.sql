WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'percent_over_comp_band_historical') }}

)

SELECT *
FROM source
