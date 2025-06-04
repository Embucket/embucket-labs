WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'event_dictionary_simplified') }}

), renamed AS (

    SELECT
      "Metric_Name"::VARCHAR      AS metric_name,
      "Product_Owner"::VARCHAR    AS product_owner,
      "Product_Category"::VARCHAR AS product_category,
      "Stage_Lookup"::VARCHAR     AS stage_lookup
    FROM source

)

SELECT *
FROM renamed
