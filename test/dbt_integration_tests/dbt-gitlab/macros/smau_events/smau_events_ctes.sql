{%- macro smau_events_ctes(event_name, regexp_where_statements=[]) -%}

{{event_name}} AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    '{{event_name}}'         AS event_type,
    page_view_id             AS event_surrogate_key

  FROM snowplow_page_views
  WHERE TRUE 
  {% for regexp_where_statement in regexp_where_statements %}
    AND page_url_path {{regexp_where_statement.regexp_function}} '{{regexp_where_statement.regexp_pattern}}'
  {% endfor %}

)

{%- endmacro -%}
