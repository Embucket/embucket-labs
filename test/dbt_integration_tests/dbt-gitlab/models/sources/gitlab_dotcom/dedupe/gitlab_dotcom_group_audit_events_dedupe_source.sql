{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'group_audit_events') }}
{% if is_incremental() %}

WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1
