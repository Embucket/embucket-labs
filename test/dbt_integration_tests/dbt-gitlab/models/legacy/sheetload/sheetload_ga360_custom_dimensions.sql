WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ga360_custom_dimensions_source') }}

)

SELECT *
FROM source
