{{ config({
    "materialized": "incremental",
    "unique_key": "note_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


{% set fields_to_mask = ['note','note_html'] %}

-- depends_on: {{ ref('internal_gitlab_namespaces') }}

WITH base AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes') }}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

, projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
)

, internal_namespaces AS (

    SELECT
      namespace_id,
      namespace_ultimate_parent_id,
      (namespace_ultimate_parent_id IN {{ get_internal_parent_namespaces() }}) AS namespace_is_internal
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, system_note_metadata AS (

    SELECT
      note_id,
      ARRAY_AGG(action_type) WITHIN GROUP (ORDER BY action_type ASC) AS action_type_array
    FROM {{ ref('gitlab_dotcom_system_note_metadata') }}
    GROUP BY 1

)

,  anonymised AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_notes'), except=fields_to_mask, relation_alias='base') }},
      {% for field in fields_to_mask %}
        CASE
          WHEN TRUE
            AND projects.visibility_level != 'public'
            AND NOT internal_namespaces.namespace_is_internal
            THEN 'confidential - masked'
          ELSE {{field}}
        END                                                    AS {{field}},
      {% endfor %}
      projects.ultimate_parent_id,
      action_type_array
    FROM base
      LEFT JOIN projects
        ON base.project_id = projects.project_id
      LEFT JOIN internal_namespaces
        ON projects.namespace_id = internal_namespaces.namespace_id
      LEFT JOIN system_note_metadata
        ON base.note_id = system_note_metadata.note_id

)

SELECT *
FROM anonymised
