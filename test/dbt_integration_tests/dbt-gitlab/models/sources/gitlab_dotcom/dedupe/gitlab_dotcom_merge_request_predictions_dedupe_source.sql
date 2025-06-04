{{ config({
    "materialized": "incremental",
    "unique_key": "merge_request_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'merge_request_predictions') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY merge_request_id ORDER BY updated_at DESC) = 1
