WITH source AS (
  SELECT *
  FROM {{ source('google_ads','campaign_history') }}
),

renamed AS (

  SELECT
    id::NUMBER AS campaign_id,
    updated_at::TIMESTAMP AS campaign_updated_at,
    customer_id::NUMBER AS customer_id,
    base_campaign_id::NUMBER AS base_campaign_id,
    ad_serving_optimization_status::VARCHAR AS campaign_ad_serving_optimization_status,
    advertising_channel_subtype::VARCHAR AS campaign_advertising_channel_subtype,
    advertising_channel_type::VARCHAR AS campaign_advertising_channel_type,
    experiment_type::VARCHAR AS campaign_experiment_type,
    end_date::VARCHAR AS campaign_end_date,
    final_url_suffix::VARCHAR AS campaign_final_url_suffix,
    frequency_caps::VARCHAR AS campaign_frequency_caps,
    name::VARCHAR AS campaign_name,
    optimization_score::FLOAT AS campaign_optimization_score,
    payment_mode::VARCHAR AS campaign_payment_mode,
    serving_status::VARCHAR AS campaign_serving_status,
    start_date::VARCHAR AS campaign_start_date,
    status::VARCHAR AS campaign_status,
    tracking_url_template::VARCHAR AS campaign_tracking_url_template,
    vanity_pharma_display_url_mode::VARCHAR AS campaign_vanity_pharma_display_url_mode,
    vanity_pharma_text::VARCHAR AS campaign_vanity_pharma_text,
    video_brand_safety_suitability::VARCHAR AS campaign_video_brand_safety_suitability,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
