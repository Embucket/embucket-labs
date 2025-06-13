WITH source AS (
  SELECT *
  FROM {{ source('google_ads','ad_group_history') }}
),

renamed AS (

  SELECT
    id::NUMBER AS ad_group_id,
    updated_at::TIMESTAMP AS ad_group_updated_at,
    campaign_id::NUMBER AS campaign_id,
    base_ad_group_id::NUMBER AS base_ad_group_id,
    ad_rotation_mode::VARCHAR AS ad_rotation_mode,
    campaign_name::VARCHAR AS campaign_name,
    display_custom_bid_dimension::VARCHAR AS ad_group_display_custom_bid_dimension,
    explorer_auto_optimizer_setting_opt_in::BOOLEAN AS is_explorer_auto_optimizer_setting_opt_in,
    final_url_suffix::VARCHAR AS ad_group_final_url_suffix,
    name::VARCHAR AS ad_group_name,
    status::VARCHAR AS ad_group_status,
    target_restrictions::VARCHAR AS ad_group_target_restrictions,
    tracking_url_template::VARCHAR AS ad_group_tracking_url_template,
    type::VARCHAR AS ad_group_type,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
