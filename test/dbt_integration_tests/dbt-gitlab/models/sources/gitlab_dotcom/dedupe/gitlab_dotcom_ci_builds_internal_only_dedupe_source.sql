{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "alias": "gitlab_dotcom_ci_builds_internal_only_dedupe_source",
    "on_schema_change": "sync_all_columns"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_builds_internal_only') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
