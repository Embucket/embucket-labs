WITH source AS (

  SELECT *
  FROM {{ source('linkedin_ads','ad_analytics_by_creative') }}

),

renamed AS (

  SELECT
    creative_id::NUMBER AS creative_id,
	day::TIMESTAMP AS day,
	clicks::NUMBER AS clicks,
	impressions::NUMBER AS impressions,
	one_click_leads::NUMBER AS one_click_leads,
	opens::NUMBER AS opens,
	cost_in_usd::NUMBER AS cost_in_usd,
	_fivetran_synced::TIMESTAMP AS _fivetran_synced

  FROM source
  
)

SELECT *
FROM renamed