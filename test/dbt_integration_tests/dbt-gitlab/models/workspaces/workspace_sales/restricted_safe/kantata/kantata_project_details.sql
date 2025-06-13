{{ config(
    {
        "materialized": "view",
        "tags": ["mnpi"]
    }
) }}


WITH source AS (
  SELECT * FROM

    {{ ref( 'kantata_project_details_source') }}
)

SELECT *
FROM source
