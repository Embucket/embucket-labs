{{ config(
    {
        "materialized": "view",
        "tags": ["mnpi"]
    }
) }}


WITH source AS (
  SELECT * FROM

    {{ ref( 'kantata_weekly_forecast_source') }}
)

SELECT *
FROM source
