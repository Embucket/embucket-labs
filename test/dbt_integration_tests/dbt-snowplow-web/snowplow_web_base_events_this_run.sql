{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run(
    sessions_this_run_table='snowplow_web_base_sessions_this_run',
    session_identifiers=var('snowplow__session_identifiers', [{"schema" : "atomic", "field" : "domain_sessionid"}]),
    session_sql=var('snowplow__session_sql', none),
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_id', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_schema=var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_table=var('snowplow__events_table', 'events')) %}

with base_query as (
  {{ base_events_query }}
)

select
  -- a.contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar as page_view_id, -- temporary fix to keep going
  COALESCE(
            GET_PATH(TRY_PARSE_JSON(a.contexts_com_snowplowanalytics_snowplow_web_page_1), '[0]:id')::VARCHAR,
            NULL
        ) as page_view_id,
  a.session_identifier as domain_sessionid,
  a.domain_sessionid as original_domain_sessionid,
  a.user_identifier as domain_userid,
  a.domain_userid as original_domain_userid,
  a.* exclude(contexts_com_snowplowanalytics_snowplow_web_page_1, domain_sessionid, domain_userid)

from base_query a
