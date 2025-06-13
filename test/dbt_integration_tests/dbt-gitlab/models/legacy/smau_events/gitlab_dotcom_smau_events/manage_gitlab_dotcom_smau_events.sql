{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "unique_key": "event_surrogate_key"
    })
}}

{%- set event_ctes = ["project_created",
                      "user_created"
                      ]
-%}

WITH project_created AS (

  SELECT
    creator_id                    AS user_id,
    TO_DATE(project_created_at)   AS event_date,
    'project_created'             AS event_type,
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_type', 'project_id']) }}
                                  AS event_surrogate_key

  FROM {{ref('gitlab_dotcom_projects_xf')}}
  WHERE project_created_at >= '2015-01-01'

)

, user_created AS (

  SELECT
    user_id,
    TO_DATE(created_at)   AS event_date,
    'user_created'             AS event_type,
    {{ dbt_utils.generate_surrogate_key(['event_date', 'event_type', 'user_id']) }}
                               AS event_surrogate_key

  FROM {{ref('gitlab_dotcom_users_xf')}}
  WHERE created_at >= '2015-01-01'

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
