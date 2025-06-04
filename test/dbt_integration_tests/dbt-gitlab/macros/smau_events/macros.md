{% docs smau_events_ctes %}
This macro is designed to build the pageview events CTEs that are then used in all the `snowplow_smau_events` models. Please [read this documentation](https://about.gitlab.com/direction/telemetry/smau_events/#events-summary-table) for more context about SMAU pageview events. This expects a CTE to exist called `snowplow_pageviews`. This CTE generally look like this:

```
{% raw %}
WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id,
    referer_url_path
  FROM {{ ref('snowplow_page_views_all') }}
  WHERE TRUE
    AND app_id = 'gitlab'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)
{% endraw %}
```

It takes 2 parameters:
* `event_name`: which is the name shown in output tables and Periscope reporting
* `regexp_where_statements`: which is a list of dictionaries. Each dictionary creates a new condition in the WHERE statement of the CTE. The dictionary will have 2 items:
  * `regexp_pattern`: the pattern that you try to match
  * `regexp_function`: the function used (either `REGEXP` or `NOT REGEXP`)
  The conditions created by a dictionary looks like: `page_url_path {regexp_function} '{regexp_pattern}'`. Conditions are always separated by an `AND`.


Output:
```
{% raw %}
pipeline_schedules_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)    AS event_date,
    page_url_path,
    'pipeline_schedules_viewed' AS event_type,
    page_view_id                AS event_surrogate_key

  FROM snowplow_page_views
  WHERE TRUE
    AND page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipeline_schedules'


)
{% endraw %}
```
{% enddocs %}
