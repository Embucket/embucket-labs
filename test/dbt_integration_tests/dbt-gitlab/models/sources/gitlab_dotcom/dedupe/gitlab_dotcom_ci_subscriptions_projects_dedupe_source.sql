{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_subscriptions_projects') }}
{% if is_incremental() %}

WHERE id >= (SELECT MAX(id) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _UPLOADED_AT DESC) = 1
