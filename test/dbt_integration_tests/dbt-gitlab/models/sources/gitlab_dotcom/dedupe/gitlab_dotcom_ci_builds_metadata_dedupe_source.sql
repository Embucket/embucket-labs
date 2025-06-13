{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "on_schema_change": "sync_all_columns"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_builds_metadata') }}
{% if is_incremental() %}

WHERE id >= (SELECT MAX(id) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _UPLOADED_AT DESC) = 1
