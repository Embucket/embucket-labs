WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_source') }}

), custom_dimensions AS (

	SELECT visit_id, visitor_id, visit_start_time, dimension_value, dimension_name
	FROM {{ ref('ga360_session_custom_dimension_xf') }}
	
), joined AS (

    SELECT 
        source.visit_id, 
        source.visitor_id, 
        source.visit_start_time,
        OBJECT_AGG(dims.dimension_name, dims.dimension_value::VARIANT)
            OVER (PARTITION BY source.visit_id, source.visitor_id, source.visit_start_time) AS custom_dimensions
    FROM source
    LEFT JOIN custom_dimensions dims
    ON dims.visit_id = source.visit_id 
            AND dims.visitor_id = source.visitor_id 
                AND dims.visit_start_time = source.visit_start_time
    GROUP BY source.visit_id, source.visitor_id, source.visit_start_time, dims.dimension_name, dims.dimension_value

), final AS (

    SELECT
      source.visit_id, 
      source.visitor_id,
      source.visit_start_time, 
      source.channel_grouping,
      source.session_date, 
      source.export_date,
      source.client_id,      
      source.visit_number,
      source.total_visits,
      source.total_pageviews,
      source.total_screenviews,
      source.total_unique_screenviews,
      source.total_hits,
      source.total_new_visits,
      source.total_time_on_screen,
      source.total_time_on_site,
      source.traffic_source,
      source.traffic_source_referral_path,
      source.traffic_source_campaign,
      source.traffic_source_medium,
      source.traffic_source_keyword,
      source.device_category,
      source.device_browser,
      source.device_browser_version,
      source.device_browser_size,
      source.device_operating_system,
      source.device_operating_system_version,
      source.geo_network_continent,
      source.geo_network_sub_continent,
      source.geo_network_country,
      source.geo_network_city,
      joined.custom_dimensions
    FROM source
    LEFT JOIN joined
    ON joined.visit_id = source.visit_id 
            AND joined.visitor_id = source.visitor_id 
                AND joined.visit_start_time = source.visit_start_time
    {{ dbt_utils.group_by(n=32)}}

)

SELECT *
FROM final
