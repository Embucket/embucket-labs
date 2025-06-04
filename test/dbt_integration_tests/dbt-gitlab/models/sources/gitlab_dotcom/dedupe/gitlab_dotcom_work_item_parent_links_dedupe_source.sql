
{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


SELECT *
FROM {{ source('gitlab_dotcom', 'work_item_parent_links') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
