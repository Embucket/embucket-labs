WITH source AS (

    SELECT *
    FROM {{ source('google_analytics_360', 'ga_session') }}

), renamed AS(

    SELECT
      --Keys
      visit_id::FLOAT                               AS visit_id, 
      visitor_id::VARCHAR                           AS visitor_id,
      visit_start_time::TIMESTAMP_TZ                AS visit_start_time, 

      --Info
      visit_start_time::DATE                        AS session_date, 
      date::DATE                                    AS export_date,
      client_id::VARCHAR                            AS client_id,      
      visit_number::FLOAT                           AS visit_number,
      channel_grouping::VARCHAR                     AS channel_grouping,
      total_visits::FLOAT                           AS total_visits,
      total_pageviews::FLOAT                        AS total_pageviews,
      total_screenviews::FLOAT                      AS total_screenviews,
      total_unique_screenviews::FLOAT               AS total_unique_screenviews,
      total_hits::FLOAT                             AS total_hits,
      total_new_visits::FLOAT                       AS total_new_visits,
      total_time_on_screen::FLOAT                   AS total_time_on_screen,
      total_time_on_site::FLOAT                     AS total_time_on_site,
      traffic_source_source::VARCHAR                AS traffic_source,
      traffic_source_referral_path::VARCHAR         AS traffic_source_referral_path,
      traffic_source_campaign::VARCHAR              AS traffic_source_campaign,
      traffic_source_medium::VARCHAR                AS traffic_source_medium,
      traffic_source_keyword::VARCHAR               AS traffic_source_keyword,
      device_device_category::VARCHAR               AS device_category,
      device_browser::VARCHAR                       AS device_browser,
      device_browser_version::VARCHAR               AS device_browser_version,
      device_browser_size::VARCHAR                  AS device_browser_size,
      device_operating_system::VARCHAR              AS device_operating_system,
      device_operating_system_version::VARCHAR      AS device_operating_system_version,
      geo_network_continent::VARCHAR                AS geo_network_continent,
      geo_network_sub_continent::VARCHAR            AS geo_network_sub_continent,
      geo_network_country::VARCHAR                  AS geo_network_country,
      geo_network_city::VARCHAR                     AS geo_network_city

    FROM source

)

SELECT *
FROM renamed
