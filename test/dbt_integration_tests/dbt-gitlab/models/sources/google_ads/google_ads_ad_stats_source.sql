WITH source AS (
  SELECT *
  FROM {{ source('google_ads','ad_stats') }}
),

renamed AS (

  SELECT
    customer_id::NUMBER AS customer_id,
    date::DATE AS ad_stats_date,
    _fivetran_id::VARCHAR AS _fivetran_id,
    campaign_base_campaign::VARCHAR AS campaign_base_campaign,
    conversions_value::FLOAT AS conversions_value,
    conversions::FLOAT AS conversions,
    interactions::NUMBER AS interactions,
    ad_id::NUMBER AS ad_id,
    ad_network_type::VARCHAR AS ad_network_type,
    interaction_event_types::VARCHAR AS interaction_event_types,
    campaign_id::NUMBER AS campaign_id,
    impressions::NUMBER AS impressions,
    active_view_viewability::FLOAT AS active_view_view_ability,
    ad_group_id::NUMBER AS ad_group_id,
    device::VARCHAR AS device,
    view_through_conversions::NUMBER AS view_through_conversions,
    active_view_impressions::NUMBER AS active_view_impressions,
    video_views::NUMBER AS video_views,
    clicks::NUMBER AS clicks,
    active_view_measurable_impressions::NUMBER AS active_view_measurable_impressions,
    active_view_measurable_cost_micros::NUMBER AS active_view_measurable_cost_micros,
    active_view_measurability::FLOAT AS active_view_measurability,
    ad_group_base_ad_group::VARCHAR AS ad_group_base_ad_group,
    cost_micros::NUMBER AS cost_micros,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
