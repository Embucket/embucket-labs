{{ config(
    {
        "materialized": "view",
        "tags": ["mnpi"]
    }
) }}


WITH source AS (
  SELECT * FROM

    {{ ref( 'kantata_remaining_to_forecast_source') }}
)

SELECT *
FROM source
