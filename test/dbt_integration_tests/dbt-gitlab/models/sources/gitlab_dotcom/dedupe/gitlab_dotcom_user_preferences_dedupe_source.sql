{{ config({
    "materialized": "incremental",
    "unique_key": "user_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'user_preferences') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1
