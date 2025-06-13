WITH source AS (
  SELECT *
  FROM {{ source('google_ads','expanded_text_ad_history') }}
),

renamed AS (

  SELECT
    ad_group_id::NUMBER AS ad_group_id,
    ad_id::NUMBER AS ad_id,
    updated_at::TIMESTAMP AS ad_text_updated_at,
    description::VARCHAR AS ad_text_description,
    description_2::VARCHAR AS ad_text_description_2,
    headline_part_1::VARCHAR AS ad_text_headline_part_1,
    headline_part_2::VARCHAR AS ad_text_headline_part_2,
    headline_part_3::VARCHAR AS ad_text_headline_part_3,
    path_1::VARCHAR AS ad_text_part_1,
    path_2::VARCHAR AS ad_text_part_2,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
