{{config({
    "materialized":"incremental",
    "incremental_strategy":"delete+insert",
    "unique_key":"session_id",
    "on_schema_change":"sync_all_columns",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

with all_web_page_views as (

    select * from {{ ref('snowplow_page_views') }}

),

relevant_sessions as (

    select distinct session_id
    from all_web_page_views

    {% if is_incremental() %}
    where page_view_start > (
            select
            COALESCE(
              MAX(session_start),
              '0001-01-01'    -- a long, long time ago
                ) as start_ts
            from {{this}} )
    {% endif %}

),
-- only select sessions that had page views in this time frame
-- this strategy helps us grab _all_ of the page views for these
-- sessions, including the ones that occurred before the
-- MAX(session_start) in the table
web_page_views AS (

    select all_web_page_views.*
    from all_web_page_views
    join relevant_sessions using (session_id)

),

prep AS (

    select
        session_id,

        -- time
        MIN(page_view_start) AS session_start,
        MAX(page_view_end) AS session_end,

        MIN(page_view_start_local) AS session_start_local,
        MAX(page_view_end_local) AS session_end_local,

        -- engagement
        count(*) AS page_views,

        SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views,

        SUM(time_engaged_in_s) AS time_engaged_in_s,

        MAX(CASE WHEN last_page_view_in_session = 1 THEN page_url ELSE NULL END)
            AS exit_page_url
        , MAX(CASE WHEN last_page_view_in_session = 1 THEN glm_source ELSE NULL END)
            AS glm_source

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_environment ELSE NULL END)
            AS gsc_environment

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_extra ELSE NULL END)
            AS gsc_extra

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_namespace_id ELSE NULL END)
            AS gsc_namespace_id

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_plan ELSE NULL END)
            AS gsc_plan

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_google_analytics_client_id ELSE NULL END)
            AS gsc_google_analytics_client_id

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_project_id ELSE NULL END)
            AS gsc_project_id

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_pseudonymized_user_id ELSE NULL END)
            AS gsc_pseudonymized_user_id

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_source ELSE NULL END)
            AS gsc_source

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN gsc_is_gitlab_team_member ELSE NULL END)
            AS gsc_is_gitlab_team_member

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_formid ELSE NULL END)
            AS cf_formid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_elementid ELSE NULL END)
            AS cf_elementid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_nodename ELSE NULL END)
            AS cf_nodename

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_type ELSE NULL END)
            AS cf_type

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_elementclasses ELSE NULL END)
            AS cf_elementclasses

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN cf_value ELSE NULL END)
            AS cf_value

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN sf_formid ELSE NULL END)
            AS sf_formid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN sf_formclasses ELSE NULL END)
            AS sf_formclasses

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN sf_elements ELSE NULL END)
            AS sf_elements

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_formid ELSE NULL END)
            AS ff_formid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_elementid ELSE NULL END)
            AS ff_elementid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_nodename ELSE NULL END)
            AS ff_nodename

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_elementtype ELSE NULL END)
            AS ff_elementtype

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_elementclasses ELSE NULL END)
            AS ff_elementclasses

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN ff_value ELSE NULL END)
            AS ff_value

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN lc_elementid ELSE NULL END)
            AS lc_elementid

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN lc_elementclasses ELSE NULL END)
            AS lc_elementclasses

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN lc_elementtarget ELSE NULL END)
            AS lc_elementtarget

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN lc_targeturl ELSE NULL END)
            AS lc_targeturl

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN lc_elementcontent ELSE NULL END)
            AS lc_elementcontent

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN tt_category ELSE NULL END)
            AS tt_category

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN tt_variable ELSE NULL END)
            AS tt_variable

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN tt_timing ELSE NULL END)
            AS tt_timing

        , MAX(CASE WHEN last_page_view_in_session = 1 THEN tt_label ELSE NULL END)
            AS tt_label

    from web_page_views

    group by 1

),

sessions AS (

    select
        -- user
        a.user_custom_id,
        a.user_snowplow_domain_id,
        a.user_snowplow_crossdomain_id,

        -- sesssion
        a.session_id,
        a.session_index,

        -- session: time
        b.session_start,
        b.session_end,

        -- session: time in the user's local timezone
        b.session_start_local,
        b.session_end_local,

        -- engagement
        b.page_views,

        b.engaged_page_views,
        b.time_engaged_in_s,

        case
            when b.time_engaged_in_s between 0 and 9 THEN '0s to 9s'
            when b.time_engaged_in_s between 10 and 29 THEN '10s to 29s'
            when b.time_engaged_in_s between 30 and 59 THEN '30s to 59s'
            when b.time_engaged_in_s between 60 and 119 THEN '60s to 119s'
            when b.time_engaged_in_s between 120 and 239 THEN '120s to 239s'
            when b.time_engaged_in_s > 239 THEN '240s or more'
            ELSE NULL
        END AS time_engaged_in_s_tier,

        CASE WHEN (b.page_views = 1) THEN true ELSE false END AS user_bounced,
        CASE WHEN (b.page_views > 2 and b.time_engaged_in_s > 59) or b.engaged_page_views > 0 THEN true ELSE false END AS user_engaged,

        -- first page

        a.page_url AS first_page_url,

        a.page_url_scheme AS first_page_url_scheme,
        a.page_url_host AS first_page_url_host,
        a.page_url_port AS first_page_url_port,
        a.page_url_path AS first_page_url_path,
        a.page_url_query AS first_page_url_query,
        a.page_url_fragment AS first_page_url_fragment,

        a.page_title AS first_page_title,

        -- last page
        b.exit_page_url,

        -- referer
        a.referer_url,

        a.referer_url_scheme,
        a.referer_url_host,
        a.referer_url_port,
        a.referer_url_path,
        a.referer_url_query,
        a.referer_url_fragment,

        a.referer_medium,
        a.referer_source,
        a.referer_term,

        -- marketing
        a.marketing_medium,
        a.marketing_source,
        a.marketing_term,
        a.marketing_content,
        a.marketing_campaign,
        a.marketing_click_id,
        a.marketing_network,

        -- location
        a.geo_country,
        a.geo_region,
        a.geo_region_name,
        a.geo_city,
        a.geo_zipcode,
        a.geo_latitude,
        a.geo_longitude,
        a.geo_timezone, -- can be NULL

        -- ip
        a.ip_address,
        a.ip_isp,
        a.ip_organization,
        a.ip_domain,
        a.ip_net_speed,

        -- application
        a.app_id,

        -- browser
        a.browser,
        a.browser_name,
        a.browser_major_version,
        a.browser_minor_version,
        a.browser_build_version,
        a.browser_engine,

        a.browser_language,

        -- os
        a.os,
        a.os_name,
        a.os_major_version,
        a.os_minor_version,
        a.os_build_version,
        a.os_manufacturer,
        a.os_timezone,

        -- device
        a.device,
        a.device_type,
        a.device_is_mobile
        , a.glm_source AS first_glm_source
        , b.glm_source AS last_glm_source

        , a.gsc_environment AS first_gsc_environment
        , b.gsc_environment AS last_gsc_environment

        , a.gsc_extra AS first_gsc_extra
        , b.gsc_extra AS last_gsc_extra

        , a.gsc_namespace_id AS first_gsc_namespace_id
        , b.gsc_namespace_id AS last_gsc_namespace_id

        , a.gsc_plan AS first_gsc_plan
        , b.gsc_plan AS last_gsc_plan

        , a.gsc_google_analytics_client_id AS first_gsc_google_analytics_client_id
        , b.gsc_google_analytics_client_id AS last_gsc_google_analytics_client_id

        , a.gsc_project_id AS first_gsc_project_id
        , b.gsc_project_id AS last_gsc_project_id

        , a.gsc_pseudonymized_user_id AS first_gsc_pseudonymized_user_id
        , b.gsc_pseudonymized_user_id AS last_gsc_pseudonymized_user_id

        , a.gsc_source AS first_gsc_source
        , b.gsc_source AS last_gsc_source

        , a.gsc_is_gitlab_team_member AS first_gsc_is_gitlab_team_member
        , b.gsc_is_gitlab_team_member AS last_gsc_is_gitlab_team_member

        , a.cf_formid AS first_cf_formid
        , b.cf_formid AS last_cf_formid

        , a.cf_elementid AS first_cf_elementid
        , b.cf_elementid AS last_cf_elementid

        , a.cf_nodename AS first_cf_nodename
        , b.cf_nodename AS last_cf_nodename

        , a.cf_type AS first_cf_type
        , b.cf_type AS last_cf_type

        , a.cf_elementclasses AS first_cf_elementclasses
        , b.cf_elementclasses AS last_cf_elementclasses

        , a.cf_value AS first_cf_value
        , b.cf_value AS last_cf_value

        , a.sf_formid AS first_sf_formid
        , b.sf_formid AS last_sf_formid

        , a.sf_formclasses AS first_sf_formclasses
        , b.sf_formclasses AS last_sf_formclasses

        , a.sf_elements AS first_sf_elements
        , b.sf_elements AS last_sf_elements

        , a.ff_formid AS first_ff_formid
        , b.ff_formid AS last_ff_formid

        , a.ff_elementid AS first_ff_elementid
        , b.ff_elementid AS last_ff_elementid

        , a.ff_nodename AS first_ff_nodename
        , b.ff_nodename AS last_ff_nodename

        , a.ff_elementtype AS first_ff_elementtype
        , b.ff_elementtype AS last_ff_elementtype

        , a.ff_elementclasses AS first_ff_elementclasses
        , b.ff_elementclasses AS last_ff_elementclasses

        , a.ff_value AS first_ff_value
        , b.ff_value AS last_ff_value

        , a.lc_elementid AS first_lc_elementid
        , b.lc_elementid AS last_lc_elementid

        , a.lc_elementclasses AS first_lc_elementclasses
        , b.lc_elementclasses AS last_lc_elementclasses

        , a.lc_elementtarget AS first_lc_elementtarget
        , b.lc_elementtarget AS last_lc_elementtarget

        , a.lc_targeturl AS first_lc_targeturl
        , b.lc_targeturl AS last_lc_targeturl

        , a.lc_elementcontent AS first_lc_elementcontent
        , b.lc_elementcontent AS last_lc_elementcontent

        , a.tt_category AS first_tt_category
        , b.tt_category AS last_tt_category

        , a.tt_variable AS first_tt_variable
        , b.tt_variable AS last_tt_variable

        , a.tt_timing AS first_tt_timing
        , b.tt_timing AS last_tt_timing

        , a.tt_label AS first_tt_label
        , b.tt_label AS last_tt_label

    from web_page_views AS a
        inner join prep AS b on a.session_id = b.session_id

    where a.page_view_in_session_index = 1

)

select * from sessions
