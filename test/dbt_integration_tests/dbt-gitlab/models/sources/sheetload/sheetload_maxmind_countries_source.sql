WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'maxmind_countries') }}

), parsed AS (

    SELECT
      geoname_id::NUMBER            AS geoname_id,
      locale_code::VARCHAR          AS locale_code,
      continent_code::VARCHAR       AS continent_code,
      continent_name::VARCHAR       AS continent_name,
      country_iso_code::VARCHAR     AS country_iso_code,
      country_name::VARCHAR         AS country_name,
      is_in_european_union::BOOLEAN AS is_in_european_union
    FROM source

)

SELECT *
FROM parsed