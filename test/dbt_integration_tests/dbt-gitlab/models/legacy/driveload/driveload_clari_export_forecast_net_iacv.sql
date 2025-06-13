WITH source AS (

    SELECT *
    FROM {{ ref('driveload_clari_export_forecast_net_iacv_source') }}

)

SELECT *
FROM source
