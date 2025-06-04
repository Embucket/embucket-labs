WITH source AS (

  SELECT 

    account_id::VARCHAR AS account_id,
    fy25_business_unit::VARCHAR AS fy25_business_unit,
    fy25_geo::VARCHAR AS fy25_geo,
    fy25_segment::VARCHAR AS fy25_segment,
    fy25_region::VARCHAR AS fy25_region,
    fy25_area::VARCHAR AS fy25_area

  FROM {{ source('sheetload','fy25_account_geo_map') }}

        )
SELECT * 
FROM source
