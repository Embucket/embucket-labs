{{ config({
    "materialized": "incremental",
    "unique_key": "namespace_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'namespace_settings') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) = 1
