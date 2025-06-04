WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'fy25_yearlies_target') }}

), renamed AS (

    SELECT
      yearly_name::VARCHAR                     AS yearly_name,
      yearly_dri::VARCHAR                      AS yearly_dri,
      yearly_description::VARCHAR              AS yearly_description,
      true::BOOLEAN                         AS is_mnpi,
      FY25_Q4::FLOAT                           AS FY25_Q4,
      FY25_Q3::FLOAT                           AS FY25_Q3,
      FY25_Q2::FLOAT                           AS FY25_Q2,
      FY25_Q1::FLOAT                           AS FY25_Q1
    FROM source

)

SELECT *
FROM renamed
