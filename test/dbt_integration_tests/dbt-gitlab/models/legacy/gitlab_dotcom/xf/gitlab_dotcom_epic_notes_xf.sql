{{ config({
    "materialized": "incremental",
    "unique_key": "note_id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}


{% set fields_to_mask = ['note'] %}

-- depends_on: {{ ref('internal_gitlab_namespaces') }}

WITH base AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes') }}
    WHERE noteable_type = 'Epic'
    {% if is_incremental() %}

      AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

, epics AS (

    SELECT *
    FROM {{ ref('prep_epic') }}
)

, namespaces AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE
)

, internal_namespaces AS (

    SELECT
      namespace_id,
      dim_namespace_sk,
      ultimate_parent_namespace_id AS namespace_ultimate_parent_id,
      (ultimate_parent_namespace_id IN {{ get_internal_parent_namespaces() }}) AS namespace_is_internal
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE


)

, anonymised AS (

    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_notes'), except=fields_to_mask, relation_alias='base') }},
      {% for field in fields_to_mask %}
        CASE
          WHEN TRUE
            AND namespaces.visibility_level != 'public'
            AND NOT internal_namespaces.namespace_is_internal
            THEN 'confidential - masked'
          ELSE {{field}}
        END AS {{field}},
      {% endfor %}
      epics.ultimate_parent_namespace_id AS ultimate_parent_id
    FROM base
      LEFT JOIN epics
        ON base.noteable_id = epics.epic_id
      LEFT JOIN namespaces
        ON epics.dim_namespace_sk = namespaces.dim_namespace_sk
      LEFT JOIN internal_namespaces
        ON epics.dim_namespace_sk = internal_namespaces.dim_namespace_sk

)

SELECT *
FROM anonymised
