WITH source AS (
  SELECT *
  FROM {{ source('google_ads','ad_history') }}
),

renamed AS (

  SELECT
    ad_group_id::NUMBER AS ad_group_id,
    id::NUMBER AS ad_id,
    updated_at::TIMESTAMP AS ad_updated_at,
    action_items::VARCHAR AS ad_action_items,
    ad_strength::VARCHAR AS ad_strength,
    added_by_google_ads::BOOLEAN AS is_added_by_google_ads,
    device_preference::VARCHAR AS ad_device_preference,
    display_url::VARCHAR AS ad_display_url,
    final_url_suffix::VARCHAR AS ad_final_url_suffix,
    final_app_urls::VARCHAR AS ad_final_app_urls,
    final_mobile_urls::VARCHAR AS ad_final_mobile_urls,
    final_urls::VARCHAR AS ad_final_urls,
    name::VARCHAR AS ad_name,
    policy_summary_approval_status::VARCHAR AS ad_policy_summary_approval_status,
    policy_summary_review_status::VARCHAR AS ad_policy_summary_review_status,
    status::VARCHAR AS ad_status,
    system_managed_resource_source::VARCHAR AS ad_system_managed_resource_source,
    tracking_url_template::VARCHAR AS tracking_url_template,
    type::VARCHAR AS ad_type,
    url_collections::VARCHAR AS ad_url_collections,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
