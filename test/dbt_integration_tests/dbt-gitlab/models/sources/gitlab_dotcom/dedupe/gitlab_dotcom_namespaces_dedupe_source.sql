{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "on_schema_change" : "sync_all_columns"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'namespaces') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
