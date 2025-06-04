WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_percent_over_comp_band_historical_source') }}

)

SELECT *
FROM source
