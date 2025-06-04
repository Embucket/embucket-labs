WITH source AS (

  SELECT *
  FROM {{ source('linkedin_ads','creative_history') }}

),

renamed AS (

  SELECT
	id::NUMBER AS creative_id,
	last_modified_time::TIMESTAMP AS last_modified_time,
	type::VARCHAR AS type,
	created_time::TIMESTAMP AS created_time,
	status::VARCHAR AS status,
	click_uri::VARCHAR AS click_uri,
	text_ad_text::VARCHAR AS text_ad_text,
	text_ad_title::VARCHAR AS text_ad_title,
	campaign_id::NUMBER AS campaign_id,
	_fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
  
)

SELECT *
FROM renamed