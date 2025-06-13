WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_clari_forecast_source') }}

)

SELECT *
FROM source
