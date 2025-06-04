

{% set fields_to_mask = ['environment_name'] %}

WITH base AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_environments') }}

)

, projects AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
)

, internal_namespaces AS (
  
    SELECT 
      namespace_id,
      namespace_is_internal
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, anonymised AS (
    
    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_environments'), except=fields_to_mask, relation_alias='base') }},
      {% for field in fields_to_mask %}
      CASE
        WHEN TRUE 
          AND projects.visibility_level != 'public'
          AND NOT internal_namespaces.namespace_is_internal
          THEN 'confidential - masked'
        ELSE {{field}}
      END AS {{field}}
      {% if not loop.last %} , {% endif %}
      {% endfor %}
    FROM base
      LEFT JOIN projects
        ON base.project_id = projects.project_id
      LEFT JOIN internal_namespaces
        ON projects.namespace_id = internal_namespaces.namespace_id

)

SELECT * 
FROM anonymised
