WITH source AS (

  SELECT *
  FROM {{ source('google_analytics_4_bigquery','pseudonymous_users') }}

),

flattened AS (

  SELECT

    value['date_part']::NUMBER                                                     AS date_part_nodash,
    value['device']['category']::VARCHAR                                           AS device_category,
    value['device']['mobile_brand_name']::VARCHAR                                  AS device_mobile_brand_name,
    value['device']['operating_system']::VARCHAR                                   AS device_operating_system,
    value['device']['unified_screen_name']::VARCHAR                                AS device_unified_screen_name,
    TO_TIMESTAMP(value['gcs_export_time']::VARCHAR)                                AS gcs_export_time,
    value['geo']['city']::VARCHAR                                                  AS geo_city,
    value['geo']['continent']::VARCHAR                                             AS geo_continent,
    value['geo']['country']::VARCHAR                                               AS geo_country,
    value['geo']['region']::VARCHAR                                                AS geo_region,
    TO_TIMESTAMP(value['last_updated_date']::VARCHAR)                              AS last_updated_date,
    TO_TIMESTAMP(value['occurrence_date']::VARCHAR)                                AS occurrence_date,
    value['pseudo_user_id']::VARCHAR                                               AS pseudo_user_id,
    value['stream_id']::NUMBER                                                     AS user_stream_id,
    TO_TIMESTAMP(value['user_info']['last_active_timestamp_micros']::VARCHAR)      AS user_last_active_timestamp_micros,
    TO_TIMESTAMP(value['user_info']['user_first_touch_timestamp_micros']::VARCHAR) AS user_first_touch_timestamp_micros,
    value['user_ltv']['engaged_sessions']::NUMBER                                  AS user_ltv_engaged_sessions,
    value['user_ltv']['engagement_time_millis']::NUMBER                            AS user_ltv_engagement_time_millis,
    value['user_ltv']['revenue_in_usd']::NUMBER                                    AS user_ltv_revenue_in_usd,
    value['user_ltv']['session_duration_micros']::NUMBER                           AS user_ltv_session_duration_micros,
    value['user_ltv']['sessions']::NUMBER                                          AS user_ltv_sessions,
    value['user_properties']::VARIANT                                              AS variant__user_properties,
    date_part::DATE                                                                AS date_part,

  FROM source

  {% if is_incremental() %}

    WHERE date_part >= (SELECT MAX(date_part) FROM {{ this }})

  {% endif %}
)

SELECT *
FROM flattened
