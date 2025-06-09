{{
    config(
        materialized='incremental',
        unique_key='page_view_id',
        snowflake_warehouse=generate_warehouse_name('XL')
    )
}}


WITH all_events AS (

    SELECT *
    FROM {{ ref('snowplow_base_events') }}

), events AS (

    SELECT *
    FROM all_events
    {% if is_incremental() %}
        WHERE collector_tstamp > DATEADD('days', -1 * {{ var('snowplow:page_view_lookback_days') }}
         , (SELECT MAX(collector_tstamp) FROM {{this}}))
    {% endif %}

), web_page_context AS (

    SELECT *
    FROM {{ ref('snowplow_web_page_context') }}

), prep AS (

    SELECT

      ev.event_id,

      ev.user_id,
      ev.domain_userid,
      ev.network_userid,

      ev.collector_tstamp,

      ev.domain_sessionid,
      ev.domain_sessionidx,

      wp.page_view_id,

      ev.page_title,

      ev.page_url,
      ev.page_urlscheme,
      ev.page_urlhost,
      ev.page_urlport,
      ev.page_urlpath,
      ev.page_urlquery,
      ev.page_urlfragment,

      ev.refr_urlscheme,
      ev.refr_urlhost,
      ev.refr_urlport,
      ev.refr_urlpath,
      ev.refr_urlquery,
      ev.refr_urlfragment,

      ev.refr_medium,
      ev.refr_source,
      ev.refr_term,

      ev.mkt_medium,
      ev.mkt_source,
      ev.mkt_term,
      ev.mkt_content,
      ev.mkt_campaign,
      ev.mkt_clickid,
      ev.mkt_network,

      ev.geo_country,
      NULL AS geo_zipcode,
      NULL AS geo_latitude,
      NULL AS geo_longitude,
      ev.geo_region,
      ev.geo_region_name,
      ev.geo_city,
      ev.geo_timezone,

      NULL AS user_ipaddress,

      ev.ip_isp,
      ev.ip_organization,
      ev.ip_domain,
      ev.ip_netspeed,

      ev.app_id,

      ev.useragent,
      ev.br_name,
      ev.br_family,
      ev.br_version,
      ev.br_type,
      ev.br_renderengine,
      ev.br_lang,
      ev.dvce_type,
      ev.dvce_ismobile,

      ev.os_name,
      ev.os_family,
      ev.os_manufacturer,
      -- Bug identified in https://gitlab.com/gitlab-data/analytics/-/issues/16142
      replace(
        CASE
          WHEN ev.os_timezone = 'Asia/Calcutt' THEN 'Asia/Calcutta'
          WHEN ev.os_timezone = 'Asia/Rangoo' THEN 'Asia/Rangoon'
          WHEN ev.os_timezone = 'Asia/Shangh' THEN 'Asia/Shanghai'
          WHEN ev.os_timezone = 'America/Buenos_Airesnos_Aires' THEN 'America/Buenos_Aires'
          WHEN ev.os_timezone = 'Asia/SaigonMinh' THEN 'Asia/Ho_Chi_Minh'
          WHEN ev.os_timezone = 'Etc/Unknown' THEN NULL
          WHEN ev.os_timezone = 'America/A' THEN NULL
          WHEN ev.os_timezone = 'SystemV/EST5' THEN NULL
          WHEN ev.os_timezone = 'SystemV/HST10' THEN NULL
          WHEN ev.os_timezone = 'EuropeALondon' THEN 'Europe/London'
          WHEN ev.os_timezone = 'SystemV/CST6' THEN NULL
          WHEN ev.os_timezone = 'America/New_York01ix691pvh' THEN NULL
          WHEN ev.os_timezone = '9072hrct3fqlaw0xcgxjbdbltczan6sukm8evij7' THEN NULL
          WHEN ev.os_timezone = 'cbbkv8gayz' THEN NULL
          WHEN regexp_instr(ev.os_timezone,'oastify.com') != 0 THEN NULL
          WHEN ev.os_timezone = 'SystemV/MST7' THEN NULL
          ELSE ev.os_timezone
        END
        , '%2F', '/') AS os_timezone,

      ev.name_tracker, -- included to filter on
      ev.dvce_created_tstamp -- included to sort on

      {%- for column in var('snowplow:pass_through_columns') %}
        , ev.{{column}}
      {% endfor %}


    FROM events AS ev
        INNER JOIN web_page_context AS wp
          ON ev.event_id = wp.root_id

    WHERE ev.platform = 'web'
      AND ev.event_name = 'page_view'

)

    SELECT
      *
    FROM prep
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp))=1
