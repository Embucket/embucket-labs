{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) %}

{{config({
    "unique_key":"bad_event_surrogate",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH base AS (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events_source') }}
    WHERE LENGTH(JSONTEXT['errors']) > 0
      AND DATE_PART(month, JSONTEXT['failure_tstamp']::timestamp) = '{{ month_value }}'
      AND DATE_PART(year, JSONTEXT['failure_tstamp']::timestamp) = '{{ year_value }}'

), renamed AS (

    SELECT
      DISTINCT JSONTEXT['line']::VARCHAR       AS base64_event,
      TO_ARRAY(JSONTEXT['errors'])            AS error_array,
      JSONTEXT[ 'failure_tstamp']::timestamp  AS failure_timestamp,
      'GitLab'                                AS infra_source,
      uploaded_at,
      {{ dbt_utils.generate_surrogate_key(['base64_event', 'failure_timestamp','error_array']) }}
                                              AS bad_event_surrogate
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp
