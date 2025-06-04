{{ config(
    {
        "materialized": "view",
        "tags": ["mnpi"]
    }
) }}


WITH source AS (
  SELECT * FROM

    {{ ref( 'kantata_revenue_by_user_source') }}
)

SELECT *
FROM source
