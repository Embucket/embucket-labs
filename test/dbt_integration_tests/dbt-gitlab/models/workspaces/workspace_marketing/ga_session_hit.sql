// https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/1410
// Joins Google Analytics 360 Session and Hit tables. Each row is a hit with the session data, 
// which is repeated for each hit. 

{{ config(
    materialized="table"
) }}


SELECT 
    gasession.client_id AS client_id, 
    gasession.visitor_id,
    gasession.visit_id, 
    gasession.visit_start_time AS session_start_date_time, 
    gasession.visit_number AS session_index, 
    gasession.total_new_visits AS new_user,
    gasession.total_time_on_site AS session_duration,
    CASE
        WHEN traffic_source_medium LIKE '%paidsocial%' THEN 'Paid Social'
        ELSE gasession.channel_grouping
    END AS channel,
    gasession.traffic_source AS traffic_source,
    gasession.traffic_source_medium AS traffic_medium,
    gasession.traffic_source_campaign AS traffic_campaign,
    gasession.traffic_source_keyword AS traffic_keyword,
    CASE 
        WHEN traffic_source_referral_path NOT LIKE '(not set)' THEN traffic_source || traffic_source_referral_path
        ELSE '(not set)'
    END AS traffic_referral_url,
    gahit.referer AS all_referrer_url,
    gasession.device_category AS device_category,
    gasession.device_browser AS browser,
    gasession.device_browser_version AS browser_version,
    gasession.device_browser_size AS browser_size,
    gasession.device_operating_system AS operating_system,
    gasession.device_operating_system_version AS operating_system_version,
    gasession.geo_network_continent,
    gasession.geo_network_sub_continent,
    gasession.geo_network_country,
    gasession.geo_network_city,
    gasession.custom_dimensions AS session_custom_dimensions, 
    gahit.hit_number AS hit_index,
    gahit.hit_at AS hit_date_time,
    gahit.is_entrance AS is_entrance,
    gahit.is_exit AS is_exit,
    gahit.hit_type AS hit_type,
    gahit.host_name || REGEXP_REPLACE(gahit.page_path, '\\?.*', '') AS page_url,
    gahit.event_category AS event_category,
    gahit.event_action AS event_action,
    gahit.event_label AS event_label,
    gahit.custom_dimensions AS hit_custom_dimensions
FROM {{ ref('ga360_session') }} gasession
JOIN {{ ref('ga360_session_hit') }} gahit
    ON gasession.visitor_id = gahit.visitor_id
    AND gasession.visit_id = gahit.visit_id
WHERE gasession.visit_start_time >= '2022-01-01'
    AND gahit.visit_start_time >= '2022-01-01'    
    AND (event_category IS NULL
        OR event_category NOT LIKE 'Web Vitals'
        AND event_category NOT LIKE 'JavaScript Error'
        AND event_category NOT LIKE 'Demandbase'
        AND event_category NOT LIKE '6Sense')
ORDER BY gahit.hit_at
