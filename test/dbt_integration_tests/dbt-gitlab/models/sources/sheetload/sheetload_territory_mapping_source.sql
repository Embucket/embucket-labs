WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'territory_mapping') }}

), renamed AS (

    SELECT
      "Segment"::VARCHAR AS segment,
      "Region"::VARCHAR AS region,
      "Sub_Region"::VARCHAR AS sub_region,
      "Area"::VARCHAR AS area,
      "Territory"::VARCHAR AS territory
    FROM source

)

SELECT *
FROM renamed
