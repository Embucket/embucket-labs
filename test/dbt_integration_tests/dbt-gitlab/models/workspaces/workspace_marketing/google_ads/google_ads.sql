WITH find_current_campaigns AS (
  SELECT
    *,
    MAX(
      campaign_updated_at
    ) OVER (PARTITION BY campaign_id ORDER BY campaign_updated_at DESC) AS latest_update,
    latest_update = campaign_updated_at AS is_latest
  FROM {{ ref('google_ads_campaign_history_source') }}
),

current_campaigns AS (
  SELECT
    *
  FROM find_current_campaigns
  WHERE is_latest
),

find_current_ad_groups AS (
  SELECT
    *,
    MAX(
      ad_group_updated_at
    ) OVER (PARTITION BY ad_group_id ORDER BY ad_group_updated_at DESC) AS latest_update,
    latest_update = ad_group_updated_at AS is_latest
  FROM {{ ref('google_ads_ad_group_history_source') }} 
),

current_ad_group AS (
  SELECT
    *,
    MAX(
      ad_group_updated_at
    ) OVER (PARTITION BY ad_group_id ORDER BY ad_group_updated_at DESC) AS latest_update,
    latest_update = ad_group_updated_at AS is_latest
  FROM find_current_ad_groups
  WHERE is_latest
),

find_current_ads AS (
  SELECT
    *,
    MAX(
      ad_updated_at
    ) OVER (PARTITION BY ad_id ORDER BY ad_updated_at DESC) AS latest_update,
    latest_update = ad_updated_at AS is_latest
  FROM {{ ref('google_ads_ad_history_source') }} 
),

current_ads AS (
  SELECT
    *
  FROM find_current_ads
  WHERE is_latest
),

find_ad_text AS (
  SELECT
    *,
    MAX(
      ad_text_updated_at
    ) OVER (PARTITION BY ad_id ORDER BY ad_text_updated_at DESC) AS latest_update,
    latest_update = ad_text_updated_at AS is_latest
  FROM {{ ref('google_ads_expanded_text_ad_history_source') }} 
),

current_ad_text AS (
  SELECT
    *
  FROM find_ad_text
  WHERE is_latest
),

ad_stats AS (
  SELECT *
  FROM {{ ref('google_ads_ad_stats_source') }} 
)

SELECT
  /* Campaign Info */

  current_campaigns.campaign_id,
  current_campaigns.campaign_ad_serving_optimization_status,
  current_campaigns.campaign_advertising_channel_subtype,
  current_campaigns.campaign_advertising_channel_type,
  current_campaigns.campaign_experiment_type,
  current_campaigns.campaign_final_url_suffix,
  current_campaigns.campaign_frequency_caps,
  current_campaigns.campaign_name,
  current_campaigns.campaign_payment_mode,
  current_campaigns.campaign_serving_status,
  current_campaigns.campaign_status,
  current_campaigns.campaign_end_date,
  current_campaigns.campaign_start_date,

  /* Ad Group Info */
  current_ad_group.ad_group_name,
  current_ad_group.ad_group_status,
  current_ad_group.ad_group_type,

  /* Ad Info */
  current_ads.ad_action_items,
  current_ads.ad_display_url,
  current_ads.ad_final_url_suffix,
  current_ads.ad_final_app_urls,
  current_ads.ad_final_mobile_urls,
  current_ads.ad_final_urls,
  current_ads.ad_status,
  current_ads.ad_type,


  /* Ad Stats */
  ad_stats.ad_stats_date,
  ad_stats.device,
  ad_stats.conversions,
  ad_stats.interactions,
  ad_stats.impressions,
  ad_stats.clicks,
  ad_stats.conversions_value,
  ad_stats.video_views,
  ad_stats.active_view_measurable_impressions,
  ad_stats.active_view_measurable_cost_micros,
  ad_stats.active_view_measurability,
  ad_stats.cost_micros,


  /* Ad Text */
  current_ad_text.ad_text_description,
  current_ad_text.ad_text_description_2,
  current_ad_text.ad_text_headline_part_1,
  current_ad_text.ad_text_headline_part_2,
  current_ad_text.ad_text_headline_part_3,
  current_ad_text.ad_text_part_1,
  current_ad_text.ad_text_part_2

FROM ad_stats
INNER JOIN current_ads
  ON ad_stats.ad_id = current_ads.ad_id 
    AND ad_stats.ad_group_id = current_ads.ad_group_id
LEFT JOIN current_ad_text
  ON current_ads.ad_id = current_ad_text.ad_id 
    AND current_ads.ad_group_id = current_ad_text.ad_group_id
LEFT JOIN current_campaigns
  ON ad_stats.campaign_id = current_campaigns.campaign_id
LEFT JOIN current_ad_group
  ON ad_stats.ad_group_id = current_ad_group.ad_group_id
