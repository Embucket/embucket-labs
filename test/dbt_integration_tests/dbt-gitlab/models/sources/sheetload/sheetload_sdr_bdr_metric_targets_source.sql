WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','sdr_bdr_metric_targets') }}

)

SELECT * 
FROM source