{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"],
    enabled=var("snowplow__enable_consent", false) and target.type == 'snowflake' | as_bool(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    e.event_id,
    e.domain_userid,
    e.original_domain_userid,
    e.user_id,
    e.geo_country,
    e.page_view_id,
    e.domain_sessionid,
    e.original_domain_sessionid,
    e.derived_tstamp,
    e.load_tstamp,
    e.event_name,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:eventType::varchar as event_type,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:basisForProcessing::varchar as basis_for_processing,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:consentUrl::varchar as consent_url,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:consentVersion::varchar as consent_version,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:consentScopes::array as consent_scopes,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:domainsApplied::array as domains_applied,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1:gdprApplies::boolean as gdpr_applies,
    e.unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1:elapsedTime::float as cmp_load_time

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  where event_name in ('cmp_visible', 'consent_preferences')

  and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.

  {% if var("snowplow__ua_bot_filter", false) %}
      {{ filter_bots() }}
  {% endif %}

)

select
  p.event_id,
  p.domain_userid,
  p.original_domain_userid,
  p.user_id,
  p.geo_country,
  p.page_view_id,
  p.domain_sessionid,
  p.original_domain_sessionid,
  p.derived_tstamp,
  p.load_tstamp,
  p.event_name,
  p.event_type,
  p.basis_for_processing,
  p.consent_url,
  p.consent_version,
  {{ snowplow_utils.get_array_to_string('consent_scopes', 'p', ', ') }} as consent_scopes,
  {{ snowplow_utils.get_array_to_string('domains_applied', 'p', ', ') }} as domains_applied,
  coalesce(p.gdpr_applies, false) as gdpr_applies,
  p.cmp_load_time

from prep p
