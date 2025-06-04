WITH
source AS (
  SELECT * FROM
    {{ source('just_global_campaigns', 'media_buys') }}
),

dedupped AS (
  SELECT *
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY media_buy_date, region, country, campaign_name, media_buy_name, creative_name ORDER BY uploaded_at DESC) = 1
)

SELECT *
FROM dedupped
