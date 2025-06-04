WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'fmm_kpi_targets') }}

), renamed as (

    SELECT
      field_segment::VARCHAR                     AS field_segment,
      region::VARCHAR                            AS region,
      kpi::VARCHAR                               AS kpi,
      goal::NUMBER                               AS goal
    FROM source

)

SELECT *
FROM renamed
