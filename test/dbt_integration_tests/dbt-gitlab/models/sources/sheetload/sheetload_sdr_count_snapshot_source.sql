WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_count_snapshot') }}

), renamed as (

    SELECT
      "Quarter"::VARCHAR                            AS fiscal_quarter,
      "Sales_Segment"::VARCHAR                      AS sales_segment,
      "SDR_Count"::NUMERIC                          AS sdr_count
      
    FROM source
)

SELECT *
FROM renamed
