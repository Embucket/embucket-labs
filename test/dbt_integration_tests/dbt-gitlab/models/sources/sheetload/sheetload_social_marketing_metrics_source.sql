WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'social_marketing_metrics') }}

), renamed as (

    SELECT
      month::DATE                                 AS month_date,
      channel::VARCHAR                            AS channel,
      brand::VARCHAR                              AS brand,
      metric::VARCHAR                             AS metric,
      true::BOOLEAN                         AS is_organic,
      value::FLOAT                                AS value,
      source::VARCHAR                             AS source,
      source_details::VARCHAR                     AS source_details
    FROM source
)

SELECT *
FROM renamed
