alter session
set
  query_tag = 'snowplow_dbt';
create
or replace temporary view DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions__dbt_tmp as (
  with prep as (
    select
      cast(null as TEXT) session_identifier
  )
  select
    *
  from
    prep
  where
    false
);
describe table DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions__dbt_tmp;
begin;
alter session
    set
    query_tag = 'snowplow_dbt';
            insert into DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions ("SESSION_IDENTIFIER") (
    select
    "SESSION_IDENTIFIER"
    from
    DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions__dbt_tmp
    );
COMMIT;
drop
    view if exists DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions__dbt_tmp cascade;
alter session
    set
    query_tag = 'snowplow_dbt';
create
or replace temporary view DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest__dbt_tmp as (
    with prep as (
      select
        cast(null as TEXT) model,
        cast('1970-01-01' as timestamp) as last_success
    )
    select
      *
    from
      prep
    where
      false
  );
describe table DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest__dbt_tmp;
describe table DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest;
describe table "DBT_TRY_1"."ATOMIC_SNOWPLOW_MANIFEST"."SNOWPLOW_WEB_INCREMENTAL_MANIFEST";
begin;
alter session
    set
    query_tag = 'snowplow_dbt';
insert into DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest ("MODEL", "LAST_SUCCESS") (
    select
    "MODEL",
    "LAST_SUCCESS"
    from
    DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest__dbt_tmp
    );
COMMIT;
drop
    view if exists DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest__dbt_tmp cascade;
select
    min(last_success) as min_last_success,
    max(last_success) as max_last_success,
    coalesce(
            count(*),
            0
    ) as models
from
    DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest
where
    model in (
              'snowplow_web_users', 'snowplow_web_users_aggs',
              'snowplow_web_users_this_run',
              'snowplow_web_users_sessions_this_run',
              'snowplow_web_users_lasts', 'snowplow_web_user_mapping',
              'snowplow_web_sessions', 'snowplow_web_sessions_this_run',
              'snowplow_web_page_views', 'snowplow_web_pv_engaged_time',
              'snowplow_web_pv_scroll_depth',
              'snowplow_web_page_views_this_run'
        );
alter session
set
    query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_new_event_limits as (
    select
      cast('2022-08-19' as timestamp) as lower_limit,
      least(
        dateadd(
          day,
          30,
          cast('2022-08-19' as timestamp)
        ),
        convert_timezone(
          'UTC',
          convert_timezone(
            'UTC',
            current_timestamp()
          )
        ):: timestamp
      ) as upper_limit
  );
select
    lower_limit,
    upper_limit
from
    DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_new_event_limits;
select
    lower_limit,
    upper_limit,
    dateadd(day, -3, lower_limit) as session_start_limit
from
    DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_new_event_limits;
select
    dateadd(
        day,
            -730,
            cast(
                    '2022-08-19 00:00:00' as timestamp
            )
    ) as session_lookback_limit;
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest as (
    select
      *
    from
      (
        with new_events_session_ids_init as (
          select
            COALESCE(e.domain_sessionid, NULL) as session_identifier,
            max(
              COALESCE(e.domain_userid, NULL)
            ) as user_identifier,
            min(collector_tstamp) as start_tstamp,
            max(collector_tstamp) as end_tstamp
          from
            DBT_TRY_1.atomic.events e
          where
            dvce_sent_tstamp <= dateadd(day, 3, dvce_created_tstamp)
            and collector_tstamp >= cast(
              '2022-08-19 00:00:00' as timestamp
            )
            and collector_tstamp <= cast(
              '2022-09-18 00:00:00' as timestamp
            )
            and true
            and True
          group by
            1
        ),
        new_events_session_ids as (
          select
            *
          from
            new_events_session_ids_init e
          where
            session_identifier is not null
            and not exists (
              select
                1
              from
                DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions as a
              where
                a.session_identifier = e.session_identifier
            )
        ),
        session_lifecycle as (
          select
            *
          from
            new_events_session_ids
        )
        select
          sl.session_identifier,
          sl.user_identifier,
          sl.start_tstamp,
          least(
            dateadd(day, 3, sl.start_tstamp),
            sl.end_tstamp
          ) as end_tstamp
        from
          session_lifecycle sl
      )
    order by
      (
        to_date(start_tstamp)
      )
  );
alter table
    DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest cluster by (
    to_date(start_tstamp)
    );
drop
    view if exists DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest__dbt_tmp cascade;
select
    lower_limit,
    upper_limit,
    dateadd(day, -3, lower_limit) as session_start_limit
from
    DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_new_event_limits;
alter session
set
    query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_sessions_this_run as (
    select
      s.session_identifier,
      s.user_identifier,
      s.start_tstamp,
      case when s.end_tstamp > cast(
        '2022-09-18 00:00:00' as timestamp
      ) then cast(
        '2022-09-18 00:00:00' as timestamp
      ) else s.end_tstamp end as end_tstamp
    from
      DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest s
    where
      s.start_tstamp >= cast(
        '2022-08-16 00:00:00' as timestamp
      )
      and s.start_tstamp <= cast(
        '2022-09-18 00:00:00' as timestamp
      )
      and not (
        s.start_tstamp > cast(
          '2022-09-18 00:00:00' as timestamp
        )
        or s.end_tstamp < cast(
          '2022-08-19 00:00:00' as timestamp
        )
      )
  );
merge into DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_base_quarantined_sessions trg using (
    select
        session_identifier
    from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_sessions_this_run
    where
        end_tstamp = dateadd(day, 3, start_tstamp)
) src on trg.session_identifier = src.session_identifier when not matched then insert (session_identifier)
    values
        (session_identifier);
select
    min(start_tstamp) as lower_limit,
    max(end_tstamp) as upper_limit
from
    DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_sessions_this_run;
alter session
set
    query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run as (
    with base_query as (
      with identified_events AS (
        select
          COALESCE(e.domain_sessionid, NULL) as session_identifier,
          e.*
        from
          DBT_TRY_1.atomic.events e
      )
      select
        a.*,
        b.user_identifier
      from
        identified_events as a
        inner join DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_sessions_this_run as b on a.session_identifier = b.session_identifier
      where
        a.collector_tstamp <= dateadd(day, 3, b.start_tstamp)
        and a.dvce_sent_tstamp <= dateadd(day, 3, a.dvce_created_tstamp)
        and a.collector_tstamp >= cast(
          '2022-08-19 23:04:17.482000' as timestamp
        )
        and a.collector_tstamp <= cast(
          '2022-08-22 00:33:22.077000' as timestamp
        )
        and a.collector_tstamp >= b.start_tstamp
        and true qualify row_number() over (
          partition by a.event_id
          order by
            a.collector_tstamp,
            a.dvce_created_tstamp
        ) = 1
    )
    select
      a.contexts_com_snowplowanalytics_snowplow_web_page_1[0] : id :: varchar as page_view_id,
      a.session_identifier as domain_sessionid,
      a.domain_sessionid as original_domain_sessionid,
      a.user_identifier as domain_userid,
      a.domain_userid as original_domain_userid,
      a.* exclude(
        contexts_com_snowplowanalytics_snowplow_web_page_1,
        domain_sessionid, domain_userid
      )
    from
      base_query a
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_pv_engaged_time as (
    select
      ev.page_view_id,
      ev.domain_sessionid,
      max(ev.derived_tstamp) as end_tstamp,
      10 * (
        count(
          distinct(
            floor(
              date_part(
                'epoch_seconds', ev.dvce_created_tstamp
              )/ 10
            )
          )
        ) -1
      ) + 5 as engaged_time_in_s
    from
      DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run as ev
    where
      ev.event_name = 'page_ping'
      and ev.page_view_id is not null
    group by
      1,
      2
  );
alter session
set
  query_tag = 'snowplow_dbt';
    create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_pv_scroll_depth as (
    with prep as (
      select
        ev.page_view_id,
        ev.domain_sessionid,
        max(ev.doc_width) as doc_width,
        max(ev.doc_height) as doc_height,
        max(ev.br_viewwidth) as br_viewwidth,
        max(ev.br_viewheight) as br_viewheight,
        least(
          greatest(
            min(
              coalesce(ev.pp_xoffset_min, 0)
            ),
            0
          ),
          max(ev.doc_width)
        ) as hmin,
        least(
          greatest(
            max(
              coalesce(ev.pp_xoffset_max, 0)
            ),
            0
          ),
          max(ev.doc_width)
        ) as hmax,
        least(
          greatest(
            min(
              coalesce(ev.pp_yoffset_min, 0)
            ),
            0
          ),
          max(ev.doc_height)
        ) as vmin,
        least(
          greatest(
            max(
              coalesce(ev.pp_yoffset_max, 0)
            ),
            0
          ),
          max(ev.doc_height)
        ) as vmax
      from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run as ev
      where
        ev.event_name in ('page_view', 'page_ping')
        and ev.page_view_id is not null
        and ev.doc_height > 0
        and ev.doc_width > 0
      group by
        1,
        2
    )
    select
      page_view_id,
      domain_sessionid,
      doc_width,
      doc_height,
      br_viewwidth,
      br_viewheight,
      hmin,
      hmax,
      vmin,
      vmax,
      cast(
        round(
          100 *(
            greatest(hmin, 0)/ cast(doc_width as float)
          )
        ) as float
      ) as relative_hmin,
      cast(
        round(
          100 *(
            least(hmax + br_viewwidth, doc_width)/ cast(doc_width as float)
          )
        ) as float
      ) as relative_hmax,
      cast(
        round(
          100 *(
            greatest(vmin, 0)/ cast(doc_height as float)
          )
        ) as float
      ) as relative_vmin,
      cast(
        round(
          100 *(
            least(vmax + br_viewheight, doc_height)/ cast(doc_height as float)
          )
        ) as float
      ) as relative_vmax
    from
      prep
  );
alter session
set
  query_tag = 'snowplow_dbt'; create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_sessions_this_run as (
    with session_firsts as (
      select
        app_id as app_id,
        platform,
        domain_sessionid,
        original_domain_sessionid,
        domain_sessionidx,
        convert_timezone(
          'UTC',
          convert_timezone(
            'UTC',
            current_timestamp()
          )
        ):: timestamp as model_tstamp,
        user_id,
        domain_userid,
        original_domain_userid,
        cast(domain_userid as TEXT) as stitched_user_id,
        network_userid as network_userid,
        page_title as first_page_title,
        page_url as first_page_url,
        page_urlscheme as first_page_urlscheme,
        page_urlhost as first_page_urlhost,
        page_urlpath as first_page_urlpath,
        page_urlquery as first_page_urlquery,
        page_urlfragment as first_page_urlfragment,
        page_referrer as referrer,
        refr_urlscheme as refr_urlscheme,
        refr_urlhost as refr_urlhost,
        refr_urlpath as refr_urlpath,
        refr_urlquery as refr_urlquery,
        refr_urlfragment as refr_urlfragment,
        refr_medium as refr_medium,
        refr_source as refr_source,
        refr_term as refr_term,
        mkt_medium as mkt_medium,
        mkt_source as mkt_source,
        mkt_term as mkt_term,
        mkt_content as mkt_content,
        mkt_campaign as mkt_campaign,
        mkt_clickid as mkt_clickid,
        mkt_network as mkt_network,
        regexp_substr(
          page_urlquery, 'utm_source_platform=([^?&#]*)',
          1, 1, 'e'
        ) as mkt_source_platform,
        case when lower(
          trim(mkt_source)
        ) = '(direct)'
        and lower(
          trim(mkt_medium)
        ) in ('(not set)', '(none)') then 'Direct' when lower(
          trim(mkt_medium)
        ) like '%cross-network%' then 'Cross-network' when regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '^(.*cp.*|ppc|retargeting|paid.*)$'
        ) then case when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
        or regexp_like(
          lower(
            trim(mkt_campaign)
          ),
          '^(.*(([^a-df-z]|^)shop|shopping).*)$'
        ) then 'Paid Shopping' when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search' when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social' when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video' else 'Paid Other' end when lower(
          trim(mkt_medium)
        ) in (
          'display', 'banner', 'expandable',
          'intersitial', 'cpm'
        ) then 'Display' when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
        or regexp_like(
          lower(
            trim(mkt_campaign)
          ),
          '^(.*(([^a-df-z]|^)shop|shopping).*)$'
        ) then 'Organic Shopping' when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL'
        or lower(
          trim(mkt_medium)
        ) in (
          'social', 'social-network', 'sm',
          'social network', 'social media'
        ) then 'Organic Social' when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
        or regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '^(.*video.*)$'
        ) then 'Organic Video' when upper(source_category) = 'SOURCE_CATEGORY_SEARCH'
        or lower(
          trim(mkt_medium)
        ) = 'organic' then 'Organic Search' when lower(
          trim(mkt_medium)
        ) in ('referral', 'app', 'link') then 'Referral' when lower(
          trim(mkt_source)
        ) in (
          'email', 'e-mail', 'e_mail', 'e mail'
        )
        or lower(
          trim(mkt_medium)
        ) in (
          'email', 'e-mail', 'e_mail', 'e mail'
        ) then 'Email' when lower(
          trim(mkt_medium)
        ) = 'affiliate' then 'Affiliates' when lower(
          trim(mkt_medium)
        ) = 'audio' then 'Audio' when lower(
          trim(mkt_source)
        ) = 'sms'
        or lower(
          trim(mkt_medium)
        ) = 'sms' then 'SMS' when lower(
          trim(mkt_medium)
        ) like '%push'
        or regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '.*(mobile|notification).*'
        )
        or lower(
          trim(mkt_source)
        ) = 'firebase' then 'Mobile Push Notifications' else 'Unassigned' end as default_channel_group,
        geo_country as geo_country,
        geo_region as geo_region,
        geo_region_name as geo_region_name,
        geo_city as geo_city,
        geo_zipcode as geo_zipcode,
        geo_latitude as geo_latitude,
        geo_longitude as geo_longitude,
        geo_timezone as geo_timezone,
        g.name as geo_country_name,
        g.region as geo_continent,
        user_ipaddress as user_ipaddress,
        useragent as useragent,
        dvce_screenwidth || 'x' || dvce_screenheight as screen_resolution,
        br_renderengine as br_renderengine,
        br_lang as br_lang,
        l.name as br_lang_name,
        os_timezone as os_timezone,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : category :: VARCHAR as category,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : primaryImpact :: VARCHAR as primary_impact,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : reason :: VARCHAR as reason,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : spiderOrRobot :: BOOLEAN as spider_or_robot,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentFamily :: VARCHAR as useragent_family,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentMajor :: VARCHAR as useragent_major,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentMinor :: VARCHAR as useragent_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentPatch :: VARCHAR as useragent_patch,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentVersion :: VARCHAR as useragent_version,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osFamily :: VARCHAR as os_family,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osMajor :: VARCHAR as os_major,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osMinor :: VARCHAR as os_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osPatch :: VARCHAR as os_patch,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osPatchMinor :: VARCHAR as os_patch_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osVersion :: VARCHAR as os_version,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : deviceFamily :: VARCHAR as device_family,
        contexts_nl_basjes_yauaa_context_1[0] : deviceClass :: VARCHAR as device_class,
        contexts_nl_basjes_yauaa_context_1[0] : agentClass :: VARCHAR as agent_class,
        contexts_nl_basjes_yauaa_context_1[0] : agentName :: VARCHAR as agent_name,
        contexts_nl_basjes_yauaa_context_1[0] : agentNameVersion :: VARCHAR as agent_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : agentNameVersionMajor :: VARCHAR as agent_name_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : agentVersion :: VARCHAR as agent_version,
        contexts_nl_basjes_yauaa_context_1[0] : agentVersionMajor :: VARCHAR as agent_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : deviceBrand :: VARCHAR as device_brand,
        contexts_nl_basjes_yauaa_context_1[0] : deviceName :: VARCHAR as device_name,
        contexts_nl_basjes_yauaa_context_1[0] : deviceVersion :: VARCHAR as device_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineClass :: VARCHAR as layout_engine_class,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineName :: VARCHAR as layout_engine_name,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineNameVersion :: VARCHAR as layout_engine_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineNameVersionMajor :: VARCHAR as layout_engine_name_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineVersion :: VARCHAR as layout_engine_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineVersionMajor :: VARCHAR as layout_engine_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemClass :: VARCHAR as operating_system_class,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemName :: VARCHAR as operating_system_name,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemNameVersion :: VARCHAR as operating_system_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemVersion :: VARCHAR as operating_system_version,
        event_name
      from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run ev
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_ga4_source_categories c on lower(
          trim(ev.mkt_source)
        ) = lower(c.source)
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_rfc_5646_language_mapping l on lower(ev.br_lang) = lower(l.lang_tag)
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_geo_country_mapping g on lower(ev.geo_country) = lower(g.alpha_2)
      where
        event_name in ('page_ping', 'page_view')
        and page_view_id is not null
        and not rlike(
          lower(useragent),
          '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*'
        ) qualify row_number() over (
          partition by domain_sessionid
          order by
            derived_tstamp,
            dvce_created_tstamp,
            event_id
        ) = 1
    ),
    session_lasts as (
      select
        domain_sessionid,
        page_title as last_page_title,
        page_url as last_page_url,
        page_urlscheme as last_page_urlscheme,
        page_urlhost as last_page_urlhost,
        page_urlpath as last_page_urlpath,
        page_urlquery as last_page_urlquery,
        page_urlfragment as last_page_urlfragment,
        geo_country as last_geo_country,
        geo_city as last_geo_city,
        geo_region_name as last_geo_region_name,
        g.name as last_geo_country_name,
        g.region as last_geo_continent,
        br_lang as last_br_lang,
        l.name as last_br_lang_name
      from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run ev
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_rfc_5646_language_mapping l on lower(ev.br_lang) = lower(l.lang_tag)
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_geo_country_mapping g on lower(ev.geo_country) = lower(g.alpha_2)
      where
        event_name = 'page_view'
        and page_view_id is not null
        and not rlike(
          lower(useragent),
          '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*'
        ) qualify row_number() over (
          partition by domain_sessionid
          order by
            derived_tstamp desc,
            dvce_created_tstamp desc,
            event_id
        ) = 1
    ),
    session_aggs as (
      select
        domain_sessionid,
        min(derived_tstamp) as start_tstamp,
        max(derived_tstamp) as end_tstamp,
        count(*) as total_events
        ,
        count(
          distinct case when event_name in ('page_ping', 'page_view')
          and page_view_id is not null then page_view_id else null end
        ) as page_views
        ,
        (
          10 * (
            count(
              distinct case when event_name = 'page_ping'
              and page_view_id is not null then
              page_view_id || cast(
                floor(
                  date_part(
                    'epoch_seconds', dvce_created_tstamp
                  )/ 10
                ) as TEXT
              ) else null end
            ) - count(
              distinct case when event_name = 'page_ping'
              and page_view_id is not null then page_view_id else null end
            )
          )
        ) +
        (
          count(
            distinct case when event_name = 'page_ping'
            and page_view_id is not null then page_view_id else null end
          ) * 5
        ) as engaged_time_in_s,
        datediff(
          second,
          min(derived_tstamp),
          max(derived_tstamp)
        ) as absolute_time_in_s
      from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run
      where
        1 = 1
        and not rlike(
          lower(useragent),
          '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*'
        )
      group by
        domain_sessionid
    )
    select
      a.app_id,
      a.platform,
      a.domain_sessionid,
      a.original_domain_sessionid,
      a.domain_sessionidx,
      case when a.event_name = 'page_ping' then dateadd(second, -5, c.start_tstamp) else c.start_tstamp end as start_tstamp,
      c.end_tstamp,
      a.model_tstamp,
      a.user_id,
      a.domain_userid,
      a.original_domain_userid,
      a.stitched_user_id,
      a.network_userid,
      c.page_views,
      c.engaged_time_in_s,
      c.total_events,
      page_views >= 2
      or engaged_time_in_s / 10 >= 2 as is_engaged,
      c.absolute_time_in_s + case when a.event_name = 'page_ping' then 5 else 0 end as absolute_time_in_s,
      a.first_page_title,
      a.first_page_url,
      a.first_page_urlscheme,
      a.first_page_urlhost,
      a.first_page_urlpath,
      a.first_page_urlquery,
      a.first_page_urlfragment,
      case when b.last_page_url is null then coalesce(
        b.last_page_title, a.first_page_title
      ) else b.last_page_title end as last_page_title,
      case when b.last_page_url is null then coalesce(
        b.last_page_url, a.first_page_url
      ) else b.last_page_url end as last_page_url,
      case when b.last_page_url is null then coalesce(
        b.last_page_urlscheme, a.first_page_urlscheme
      ) else b.last_page_urlscheme end as last_page_urlscheme,
      case when b.last_page_url is null then coalesce(
        b.last_page_urlhost, a.first_page_urlhost
      ) else b.last_page_urlhost end as last_page_urlhost,
      case when b.last_page_url is null then coalesce(
        b.last_page_urlpath, a.first_page_urlpath
      ) else b.last_page_urlpath end as last_page_urlpath,
      case when b.last_page_url is null then coalesce(
        b.last_page_urlquery, a.first_page_urlquery
      ) else b.last_page_urlquery end as last_page_urlquery,
      case when b.last_page_url is null then coalesce(
        b.last_page_urlfragment, a.first_page_urlfragment
      ) else b.last_page_urlfragment end as last_page_urlfragment,
      a.referrer,
      a.refr_urlscheme,
      a.refr_urlhost,
      a.refr_urlpath,
      a.refr_urlquery,
      a.refr_urlfragment,
      a.refr_medium,
      a.refr_source,
      a.refr_term,
      a.mkt_medium,
      a.mkt_source,
      a.mkt_term,
      a.mkt_content,
      a.mkt_campaign,
      a.mkt_clickid,
      a.mkt_network,
      a.mkt_source_platform,
      a.default_channel_group,
      a.geo_country,
      a.geo_region,
      a.geo_region_name,
      a.geo_city,
      a.geo_zipcode,
      a.geo_latitude,
      a.geo_longitude,
      a.geo_timezone,
      a.geo_country_name,
      a.geo_continent,
      case when b.last_geo_country is null then coalesce(
        b.last_geo_country, a.geo_country
      ) else b.last_geo_country end as last_geo_country,
      case when b.last_geo_country is null then coalesce(
        b.last_geo_region_name, a.geo_region_name
      ) else b.last_geo_region_name end as last_geo_region_name,
      case when b.last_geo_country is null then coalesce(b.last_geo_city, a.geo_city) else b.last_geo_city end as last_geo_city,
      case when b.last_geo_country is null then coalesce(
        b.last_geo_country_name, a.geo_country_name
      ) else b.last_geo_country_name end as last_geo_country_name,
      case when b.last_geo_country is null then coalesce(
        b.last_geo_continent, a.geo_continent
      ) else b.last_geo_continent end as last_geo_continent,
      a.user_ipaddress,
      a.useragent,
      a.br_renderengine,
      a.br_lang,
      a.br_lang_name,
      case when b.last_br_lang is null then coalesce(b.last_br_lang, a.br_lang) else b.last_br_lang end as last_br_lang,
      case when b.last_br_lang is null then coalesce(
        b.last_br_lang_name, a.br_lang_name
      ) else b.last_br_lang_name end as last_br_lang_name,
      a.os_timezone,
      a.category,
      a.primary_impact,
      a.reason,
      a.spider_or_robot,
      a.useragent_family,
      a.useragent_major,
      a.useragent_minor,
      a.useragent_patch,
      a.useragent_version,
      a.os_family,
      a.os_major,
      a.os_minor,
      a.os_patch,
      a.os_patch_minor,
      a.os_version,
      a.device_family,
      a.device_class,
      case when a.device_class = 'Desktop' THEN 'Desktop' when a.device_class = 'Phone' then 'Mobile' when a.device_class = 'Tablet' then 'Tablet' else 'Other' end as device_category,
      a.screen_resolution,
      a.agent_class,
      a.agent_name,
      a.agent_name_version,
      a.agent_name_version_major,
      a.agent_version,
      a.agent_version_major,
      a.device_brand,
      a.device_name,
      a.device_version,
      a.layout_engine_class,
      a.layout_engine_name,
      a.layout_engine_name_version,
      a.layout_engine_name_version_major,
      a.layout_engine_version,
      a.layout_engine_version_major,
      a.operating_system_class,
      a.operating_system_name,
      a.operating_system_name_version,
      a.operating_system_version
    from
      session_firsts a
      left join session_lasts b on a.domain_sessionid = b.domain_sessionid
      left join session_aggs c on a.domain_sessionid = c.domain_sessionid
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_derived.snowplow_web_user_mapping as (
    select
      distinct domain_userid,
      last_value(user_id) over(
        partition by domain_userid
        order by
          collector_tstamp rows between unbounded preceding
          and unbounded following
      ) as user_id,
      max(collector_tstamp) over (partition by domain_userid) as end_tstamp
    from
      DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run
    where
      True
      and user_id is not null
      and domain_userid is not null
  );
drop
    view if exists DBT_TRY_1.ATOMIC_derived.snowplow_web_user_mapping__dbt_tmp cascade;
alter session
    set
    query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_page_views_this_run as (
    with prep as (
      select
        ev.page_view_id,
        ev.event_id,
        ev.app_id,
        ev.platform,
        ev.user_id,
        ev.domain_userid,
        ev.original_domain_userid,
        cast(null as TEXT) as stitched_user_id,
        ev.network_userid,
        ev.domain_sessionid,
        ev.original_domain_sessionid,
        ev.domain_sessionidx,
        ev.dvce_created_tstamp,
        ev.collector_tstamp,
        ev.derived_tstamp,
        ev.derived_tstamp as start_tstamp,
        ev.doc_width,
        ev.doc_height,
        ev.page_title,
        case when ev.page_url like '%/product%' then 'PDP' when ev.page_url like '%/list%' then 'PLP' when ev.page_url like '%/checkout%' then 'checkout' when ev.page_url like '%/home%' then 'homepage' else 'other' end as content_group,
        ev.page_url,
        ev.page_urlscheme,
        ev.page_urlhost,
        ev.page_urlpath,
        ev.page_urlquery,
        ev.page_urlfragment,
        ev.mkt_medium,
        ev.mkt_source,
        ev.mkt_term,
        ev.mkt_content,
        ev.mkt_campaign,
        ev.mkt_clickid,
        ev.mkt_network,
        case when lower(
          trim(mkt_source)
        ) = '(direct)'
        and lower(
          trim(mkt_medium)
        ) in ('(not set)', '(none)') then 'Direct' when lower(
          trim(mkt_medium)
        ) like '%cross-network%' then 'Cross-network' when regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '^(.*cp.*|ppc|retargeting|paid.*)$'
        ) then case when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
        or regexp_like(
          lower(
            trim(mkt_campaign)
          ),
          '^(.*(([^a-df-z]|^)shop|shopping).*)$'
        ) then 'Paid Shopping' when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search' when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social' when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video' else 'Paid Other' end when lower(
          trim(mkt_medium)
        ) in (
          'display', 'banner', 'expandable',
          'intersitial', 'cpm'
        ) then 'Display' when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
        or regexp_like(
          lower(
            trim(mkt_campaign)
          ),
          '^(.*(([^a-df-z]|^)shop|shopping).*)$'
        ) then 'Organic Shopping' when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL'
        or lower(
          trim(mkt_medium)
        ) in (
          'social', 'social-network', 'sm',
          'social network', 'social media'
        ) then 'Organic Social' when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
        or regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '^(.*video.*)$'
        ) then 'Organic Video' when upper(source_category) = 'SOURCE_CATEGORY_SEARCH'
        or lower(
          trim(mkt_medium)
        ) = 'organic' then 'Organic Search' when lower(
          trim(mkt_medium)
        ) in ('referral', 'app', 'link') then 'Referral' when lower(
          trim(mkt_source)
        ) in (
          'email', 'e-mail', 'e_mail', 'e mail'
        )
        or lower(
          trim(mkt_medium)
        ) in (
          'email', 'e-mail', 'e_mail', 'e mail'
        ) then 'Email' when lower(
          trim(mkt_medium)
        ) = 'affiliate' then 'Affiliates' when lower(
          trim(mkt_medium)
        ) = 'audio' then 'Audio' when lower(
          trim(mkt_source)
        ) = 'sms'
        or lower(
          trim(mkt_medium)
        ) = 'sms' then 'SMS' when lower(
          trim(mkt_medium)
        ) like '%push'
        or regexp_like(
          lower(
            trim(mkt_medium)
          ),
          '.*(mobile|notification).*'
        )
        or lower(
          trim(mkt_source)
        ) = 'firebase' then 'Mobile Push Notifications' else 'Unassigned' end as default_channel_group,
        ev.page_referrer,
        ev.refr_urlscheme,
        ev.refr_urlhost,
        ev.refr_urlpath,
        ev.refr_urlquery,
        ev.refr_urlfragment,
        ev.refr_medium,
        ev.refr_source,
        ev.refr_term,
        ev.geo_country,
        ev.geo_region,
        ev.geo_region_name,
        ev.geo_city,
        ev.geo_zipcode,
        ev.geo_latitude,
        ev.geo_longitude,
        ev.geo_timezone,
        ev.user_ipaddress,
        ev.useragent,
        ev.dvce_screenwidth || 'x' || ev.dvce_screenheight as screen_resolution,
        ev.br_lang,
        ev.br_viewwidth,
        ev.br_viewheight,
        ev.br_colordepth,
        ev.br_renderengine,
        ev.os_timezone,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : category :: VARCHAR as category,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : primaryImpact :: VARCHAR as primary_impact,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : reason :: VARCHAR as reason,
        contexts_com_iab_snowplow_spiders_and_robots_1[0] : spiderOrRobot :: BOOLEAN as spider_or_robot,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentFamily :: VARCHAR as useragent_family,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentMajor :: VARCHAR as useragent_major,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentMinor :: VARCHAR as useragent_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentPatch :: VARCHAR as useragent_patch,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : useragentVersion :: VARCHAR as useragent_version,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osFamily :: VARCHAR as os_family,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osMajor :: VARCHAR as os_major,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osMinor :: VARCHAR as os_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osPatch :: VARCHAR as os_patch,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osPatchMinor :: VARCHAR as os_patch_minor,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : osVersion :: VARCHAR as os_version,
        contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0] : deviceFamily :: VARCHAR as device_family,
        contexts_nl_basjes_yauaa_context_1[0] : deviceClass :: VARCHAR as device_class,
        contexts_nl_basjes_yauaa_context_1[0] : agentClass :: VARCHAR as agent_class,
        contexts_nl_basjes_yauaa_context_1[0] : agentName :: VARCHAR as agent_name,
        contexts_nl_basjes_yauaa_context_1[0] : agentNameVersion :: VARCHAR as agent_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : agentNameVersionMajor :: VARCHAR as agent_name_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : agentVersion :: VARCHAR as agent_version,
        contexts_nl_basjes_yauaa_context_1[0] : agentVersionMajor :: VARCHAR as agent_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : deviceBrand :: VARCHAR as device_brand,
        contexts_nl_basjes_yauaa_context_1[0] : deviceName :: VARCHAR as device_name,
        contexts_nl_basjes_yauaa_context_1[0] : deviceVersion :: VARCHAR as device_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineClass :: VARCHAR as layout_engine_class,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineName :: VARCHAR as layout_engine_name,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineNameVersion :: VARCHAR as layout_engine_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineNameVersionMajor :: VARCHAR as layout_engine_name_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineVersion :: VARCHAR as layout_engine_version,
        contexts_nl_basjes_yauaa_context_1[0] : layoutEngineVersionMajor :: VARCHAR as layout_engine_version_major,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemClass :: VARCHAR as operating_system_class,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemName :: VARCHAR as operating_system_name,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemNameVersion :: VARCHAR as operating_system_name_version,
        contexts_nl_basjes_yauaa_context_1[0] : operatingSystemVersion :: VARCHAR as operating_system_version
      from
        DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run as ev
        left join DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_dim_ga4_source_categories c on lower(
          trim(ev.mkt_source)
        ) = lower(c.source)
      where
        ev.event_name = 'page_view'
        and ev.page_view_id is not null
        and not rlike(
          lower(ev.useragent),
          '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*'
        ) qualify row_number() over (
          partition by ev.page_view_id
          order by
            ev.derived_tstamp,
            ev.dvce_created_tstamp
        ) = 1
    ),
    page_view_events as (
      select
        p.page_view_id,
        p.event_id,
        p.app_id,
        p.platform,
        p.user_id,
        p.domain_userid,
        p.original_domain_userid,
        p.stitched_user_id,
        p.network_userid,
        p.domain_sessionid,
        p.original_domain_sessionid,
        p.domain_sessionidx,
        row_number() over (
          partition by p.domain_sessionid
          order by
            p.derived_tstamp,
            p.dvce_created_tstamp,
            p.event_id
        ) AS page_view_in_session_index,
        p.dvce_created_tstamp,
        p.collector_tstamp,
        p.derived_tstamp,
        p.start_tstamp,
        coalesce(t.end_tstamp, p.derived_tstamp) as end_tstamp,
        convert_timezone(
          'UTC',
          convert_timezone(
            'UTC',
            current_timestamp()
          )
        ):: timestamp as model_tstamp,
        coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s,
        datediff(
          second,
          p.derived_tstamp,
          coalesce(t.end_tstamp, p.derived_tstamp)
        ) as absolute_time_in_s,
        sd.hmax as horizontal_pixels_scrolled,
        sd.vmax as vertical_pixels_scrolled,
        sd.relative_hmax as horizontal_percentage_scrolled,
        sd.relative_vmax as vertical_percentage_scrolled,
        p.doc_width,
        p.doc_height,
        p.content_group,
        p.page_title,
        p.page_url,
        p.page_urlscheme,
        p.page_urlhost,
        p.page_urlpath,
        p.page_urlquery,
        p.page_urlfragment,
        p.mkt_medium,
        p.mkt_source,
        p.mkt_term,
        p.mkt_content,
        p.mkt_campaign,
        p.mkt_clickid,
        p.mkt_network,
        p.default_channel_group,
        p.page_referrer,
        p.refr_urlscheme,
        p.refr_urlhost,
        p.refr_urlpath,
        p.refr_urlquery,
        p.refr_urlfragment,
        p.refr_medium,
        p.refr_source,
        p.refr_term,
        p.geo_country,
        p.geo_region,
        p.geo_region_name,
        p.geo_city,
        p.geo_zipcode,
        p.geo_latitude,
        p.geo_longitude,
        p.geo_timezone,
        p.user_ipaddress,
        p.useragent,
        p.screen_resolution,
        p.br_lang,
        p.br_viewwidth,
        p.br_viewheight,
        p.br_colordepth,
        p.br_renderengine,
        p.os_timezone,
        p.category,
        p.primary_impact,
        p.reason,
        p.spider_or_robot,
        p.useragent_family,
        p.useragent_major,
        p.useragent_minor,
        p.useragent_patch,
        p.useragent_version,
        p.os_family,
        p.os_major,
        p.os_minor,
        p.os_patch,
        p.os_patch_minor,
        p.os_version,
        p.device_family,
        p.device_class,
        p.agent_class,
        p.agent_name,
        p.agent_name_version,
        p.agent_name_version_major,
        p.agent_version,
        p.agent_version_major,
        p.device_brand,
        p.device_name,
        p.device_version,
        p.layout_engine_class,
        p.layout_engine_name,
        p.layout_engine_name_version,
        p.layout_engine_name_version_major,
        p.layout_engine_version,
        p.layout_engine_version_major,
        p.operating_system_class,
        p.operating_system_name,
        p.operating_system_name_version,
        p.operating_system_version
      from
        prep p
        left join DBT_TRY_1.ATOMIC_scratch.snowplow_web_pv_engaged_time t on p.page_view_id = t.page_view_id
        and p.domain_sessionid = t.domain_sessionid
        left join DBT_TRY_1.ATOMIC_scratch.snowplow_web_pv_scroll_depth sd on p.page_view_id = sd.page_view_id
        and p.domain_sessionid = sd.domain_sessionid
    )
    select
      pve.page_view_id,
      pve.event_id,
      pve.app_id,
      pve.platform,
      pve.user_id,
      pve.domain_userid,
      pve.original_domain_userid,
      pve.stitched_user_id,
      pve.network_userid,
      pve.domain_sessionid,
      pve.original_domain_sessionid,
      pve.domain_sessionidx,
      pve.page_view_in_session_index,
      max(pve.page_view_in_session_index) over (
        partition by pve.domain_sessionid
      ) as page_views_in_session,
      pve.dvce_created_tstamp,
      pve.collector_tstamp,
      pve.derived_tstamp,
      pve.start_tstamp,
      pve.end_tstamp,
      pve.model_tstamp,
      pve.engaged_time_in_s,
      pve.absolute_time_in_s,
      pve.horizontal_pixels_scrolled,
      pve.vertical_pixels_scrolled,
      pve.horizontal_percentage_scrolled,
      pve.vertical_percentage_scrolled,
      pve.doc_width,
      pve.doc_height,
      pve.content_group,
      pve.page_title,
      pve.page_url,
      pve.page_urlscheme,
      pve.page_urlhost,
      pve.page_urlpath,
      pve.page_urlquery,
      pve.page_urlfragment,
      pve.mkt_medium,
      pve.mkt_source,
      pve.mkt_term,
      pve.mkt_content,
      pve.mkt_campaign,
      pve.mkt_clickid,
      pve.mkt_network,
      pve.default_channel_group,
      pve.page_referrer,
      pve.refr_urlscheme,
      pve.refr_urlhost,
      pve.refr_urlpath,
      pve.refr_urlquery,
      pve.refr_urlfragment,
      pve.refr_medium,
      pve.refr_source,
      pve.refr_term,
      pve.geo_country,
      pve.geo_region,
      pve.geo_region_name,
      pve.geo_city,
      pve.geo_zipcode,
      pve.geo_latitude,
      pve.geo_longitude,
      pve.geo_timezone,
      pve.user_ipaddress,
      pve.useragent,
      pve.br_lang,
      pve.br_viewwidth,
      pve.br_viewheight,
      pve.br_colordepth,
      pve.br_renderengine,
      pve.os_timezone,
      pve.category,
      pve.primary_impact,
      pve.reason,
      pve.spider_or_robot,
      pve.useragent_family,
      pve.useragent_major,
      pve.useragent_minor,
      pve.useragent_patch,
      pve.useragent_version,
      pve.os_family,
      pve.os_major,
      pve.os_minor,
      pve.os_patch,
      pve.os_patch_minor,
      pve.os_version,
      pve.device_family,
      pve.device_class,
      case when pve.device_class = 'Desktop' then 'Desktop' when pve.device_class = 'Phone' then 'Mobile' when pve.device_class = 'Tablet' then 'Tablet' else 'Other' end as device_category,
      pve.screen_resolution,
      pve.agent_class,
      pve.agent_name,
      pve.agent_name_version,
      pve.agent_name_version_major,
      pve.agent_version,
      pve.agent_version_major,
      pve.device_brand,
      pve.device_name,
      pve.device_version,
      pve.layout_engine_class,
      pve.layout_engine_name,
      pve.layout_engine_name_version,
      pve.layout_engine_name_version_major,
      pve.layout_engine_version,
      pve.layout_engine_version_major,
      pve.operating_system_class,
      pve.operating_system_name,
      pve.operating_system_name_version,
      pve.operating_system_version
    from
      page_view_events pve
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_derived.snowplow_web_sessions as (
    select
      *
    from
      (
        select
          *
        from
          DBT_TRY_1.ATOMIC_scratch.snowplow_web_sessions_this_run
        where
          True
          )
    order by
      (
        to_date(start_tstamp)
      )
  );
alter table
    DBT_TRY_1.ATOMIC_derived.snowplow_web_sessions cluster by (
    to_date(start_tstamp)
    );
drop
    view if exists DBT_TRY_1.ATOMIC_derived.snowplow_web_sessions__dbt_tmp cascade;
update
    DBT_TRY_1.ATOMIC_derived.snowplow_web_sessions as s
set
    stitched_user_id = um.user_id
    from
  DBT_TRY_1.ATOMIC_derived.snowplow_web_user_mapping as um
where
    s.domain_userid = um.domain_userid;
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_derived.snowplow_web_page_views as (
    select
      *
    from
      (
        select
          *
        from
          DBT_TRY_1.ATOMIC_scratch.snowplow_web_page_views_this_run
        where
          True
          )
    order by
      (
        to_date(start_tstamp)
      )
  );
alter table
    DBT_TRY_1.ATOMIC_derived.snowplow_web_page_views cluster by (
    to_date(start_tstamp)
    );
drop
    view if exists DBT_TRY_1.ATOMIC_derived.snowplow_web_page_views__dbt_tmp cascade;
alter session
    set
    query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_sessions_this_run as (
    select
      a.*,
      min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
      max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp
    from
      DBT_TRY_1.ATOMIC_derived.snowplow_web_sessions a
    where
      exists (
        select
          1
        from
          DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_sessions_this_run b
        where
          a.domain_userid = b.user_identifier
      )
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_aggs as (
    select
      domain_userid,
      user_start_tstamp as start_tstamp,
      user_end_tstamp as end_tstamp,
      max(
        case when start_tstamp = user_start_tstamp then domain_sessionid end
      ) as first_domain_sessionid,
      max(
        case when end_tstamp = user_end_tstamp then domain_sessionid end
      ) as last_domain_sessionid,
      sum(page_views) as page_views,
      count(distinct domain_sessionid) as sessions,
      sum(engaged_time_in_s) as engaged_time_in_s
    from
      DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_sessions_this_run
    group by
      1,
      2,
      3
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_lasts as (
    select
      a.domain_userid,
      a.last_page_title,
      a.last_page_url,
      a.last_page_urlscheme,
      a.last_page_urlhost,
      a.last_page_urlpath,
      a.last_page_urlquery,
      a.last_page_urlfragment,
      a.last_geo_country,
      a.last_geo_country_name,
      a.last_geo_continent,
      a.last_geo_city,
      a.last_geo_region_name,
      a.last_br_lang,
      a.last_br_lang_name
    from
      DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_sessions_this_run a
      inner join DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_aggs b on a.domain_sessionid = b.last_domain_sessionid
  );
alter session
set
  query_tag = 'snowplow_dbt';
create
or replace transient table DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_this_run as (
    select
      a.user_id,
      a.domain_userid,
      a.original_domain_userid,
      a.network_userid,
      b.start_tstamp,
      b.end_tstamp,
      convert_timezone(
        'UTC',
        convert_timezone(
          'UTC',
          current_timestamp()
        )
      ):: timestamp as model_tstamp,
      b.page_views,
      b.sessions,
      b.engaged_time_in_s,
      a.first_page_title,
      a.first_page_url,
      a.first_page_urlscheme,
      a.first_page_urlhost,
      a.first_page_urlpath,
      a.first_page_urlquery,
      a.first_page_urlfragment,
      a.geo_country as first_geo_country,
      a.geo_country_name as first_geo_country_name,
      a.geo_continent as first_geo_continent,
      a.geo_city as first_geo_city,
      a.geo_region_name as first_geo_region_name,
      a.br_lang as first_br_lang,
      a.br_lang_name as first_br_lang_name,
      c.last_page_title,
      c.last_page_url,
      c.last_page_urlscheme,
      c.last_page_urlhost,
      c.last_page_urlpath,
      c.last_page_urlquery,
      c.last_page_urlfragment,
      c.last_geo_country,
      c.last_geo_country_name,
      c.last_geo_continent,
      c.last_geo_city,
      c.last_geo_region_name,
      c.last_br_lang,
      c.last_br_lang_name,
      a.referrer,
      a.refr_urlscheme,
      a.refr_urlhost,
      a.refr_urlpath,
      a.refr_urlquery,
      a.refr_urlfragment,
      a.refr_medium,
      a.refr_source,
      a.refr_term,
      a.mkt_medium,
      a.mkt_source,
      a.mkt_term,
      a.mkt_content,
      a.mkt_campaign,
      a.mkt_clickid,
      a.mkt_network,
      a.mkt_source_platform,
      a.default_channel_group
    from
      DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_aggs as b
      inner join DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_sessions_this_run as a on a.domain_sessionid = b.first_domain_sessionid
      inner join DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_lasts c on b.domain_userid = c.domain_userid
  );
alter session
set
  query_tag = 'snowplow_dbt';
create or replace transient table DBT_TRY_1.ATOMIC_derived.snowplow_web_users as (
    select
      *
    from
      (
        select
          *
        from
          DBT_TRY_1.ATOMIC_scratch.snowplow_web_users_this_run
        where
          True
          )
    order by
      (
        to_date(start_tstamp)
      )
  );
alter table
    DBT_TRY_1.ATOMIC_derived.snowplow_web_users cluster by (
    to_date(start_tstamp)
    );
drop
    view if exists DBT_TRY_1.ATOMIC_derived.snowplow_web_users__dbt_tmp cascade;
merge into DBT_TRY_1.ATOMIC_snowplow_manifest.snowplow_web_incremental_manifest m using (
    select
    b.model,
    a.last_success
    from
    (
    select
    max(collector_tstamp) as last_success
    from
    DBT_TRY_1.ATOMIC_scratch.snowplow_web_base_events_this_run
    ) a,
    (
    select
    'snowplow_web_pv_engaged_time' as model
    union all
    select
    'snowplow_web_pv_scroll_depth' as model
    union all
    select
    'snowplow_web_sessions_this_run' as model
    union all
    select
    'snowplow_web_user_mapping' as model
    union all
    select
    'snowplow_web_page_views_this_run' as model
    union all
    select
    'snowplow_web_sessions' as model
    union all
    select
    'snowplow_web_page_views' as model
    union all
    select
    'snowplow_web_users_sessions_this_run' as model
    union all
    select
    'snowplow_web_users_aggs' as model
    union all
    select
    'snowplow_web_users_lasts' as model
    union all
    select
    'snowplow_web_users_this_run' as model
    union all
    select
    'snowplow_web_users' as model
    ) b
    where
    a.last_success is not null
    ) s on m.model = s.model when matched then
update
    set
        last_success = greatest(m.last_success, s.last_success) when not matched then insert (model, last_success)
values
    (model, last_success);
COMMIT;
