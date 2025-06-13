WITH find_current_ads AS (
  SELECT
    *,
    MAX(
      updated_time
    ) OVER (PARTITION BY ad_id ORDER BY updated_time DESC) AS latest_update,
    latest_update = updated_time AS is_latest
  FROM {{ ref('facebook_ads_ad_history_source') }}

),current_ads AS (
  SELECT
    *
  FROM find_current_ads
  WHERE is_latest

), find_current_creatives AS (
  SELECT
    *,
    MAX(
      _FIVETRAN_SYNCED
    ) OVER (PARTITION BY creative_id ORDER BY _FIVETRAN_SYNCED DESC) AS latest_update,
    latest_update = _FIVETRAN_SYNCED AS is_latest
  FROM {{ ref('facebook_ads_creative_history_source') }}

),current_creatives AS (
  SELECT
    *
  FROM find_current_creatives
  WHERE is_latest

), basic_all_levels as (
    select * FROM {{ ref('facebook_ads_basic_all_levels_source') }}
)

select
    /* Account Info */
    basic_all_levels.account_id,
    
    /* Campaign Info */
    current_ads.campaign_id,
    basic_all_levels.campaign_name,
    basic_all_levels.adset_name,
    current_ads.ad_name              AS ad_name,
    current_ads.ad_status            AS ad_status,
    

    /* Creative Info */

    current_creatives.creative_name,
    current_creatives.OBJECT_TYPE      AS creative_type,
    current_creatives.creative_status  AS creative_status,
    current_creatives.page_link,
    current_creatives.body        AS text_ad_text,
    current_creatives.title       AS text_ad_title,
    
    /* Creative Stats */
    basic_all_levels.ad_date         AS campaign_day,
    basic_all_levels.impressions,
    basic_all_levels.inline_link_clicks,
    basic_all_levels.spend
FROM basic_all_levels
    LEFT JOIN current_ads ON basic_all_levels.ad_id = current_ads.ad_id
    LEFT JOIN current_creatives ON current_ads.CREATIVE_ID = current_creatives.CREATIVE_ID
