{{ config({
    "unique_key": "event_surrogate_key",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

{%- set event_ctes = ["mr_created",
                      "mr_comment_added",
                      "snippet_comment_added"
                      ]
-%}

WITH mr_comment_added AS (

  SELECT
    note_author_id           AS user_id,
    TO_DATE(created_at)      AS event_date,
    'mr_comment_added'       AS event_type,
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_type', 'note_id']) }}
                             AS event_surrogate_key

  FROM {{ref('gitlab_dotcom_notes')}}
  WHERE noteable_type = 'MergeRequest'
    AND created_at >= '2015-01-01'

)

, mr_created AS (

  SELECT
    author_id                         AS user_id,
    TO_DATE(created_at) AS event_date,
    'mr_created'                      AS event_type,
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_type', 'merge_request_id']) }}
                                      AS event_surrogate_key

  FROM {{ref('gitlab_dotcom_merge_requests_source')}}
  WHERE created_at >= '2015-01-01'

)

, snippet_comment_added AS (

  SELECT
    note_author_id           AS user_id,
    TO_DATE(created_at) AS event_date,
    'snippet_comment_added'  AS event_type,
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_type', 'note_id']) }}
                             AS event_surrogate_key

  FROM {{ref('gitlab_dotcom_notes')}}
  WHERE noteable_type = 'Snippet'
    AND created_at >= '2015-01-01'

)

, unioned AS (
  {% for event_cte in event_ctes %}

    SELECT *
    FROM {{ event_cte }}

    {%- if not loop.last %}
      UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
