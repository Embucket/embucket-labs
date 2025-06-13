WITH source AS (
  SELECT *
  FROM {{ source('facebook_ads','basic_all_levels') }}
),

renamed AS (

  SELECT
    ad_id::VARCHAR              AS ad_id,
    date::DATE                  AS ad_date,
    account_id::NUMBER          AS account_id,
    impressions::NUMBER         AS impressions,
    inline_link_clicks::NUMBER  AS inline_link_clicks,
    spend::FLOAT                AS spend,
    ad_name::VARCHAR            AS ad_name,
    adset_name::VARCHAR         AS adset_name,
    campaign_name::VARCHAR      AS campaign_name,
    _fivetran_id::VARCHAR       AS _fivetran_id,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
