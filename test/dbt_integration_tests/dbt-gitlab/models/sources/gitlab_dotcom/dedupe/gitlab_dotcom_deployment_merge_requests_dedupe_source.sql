{{ config({
    "materialized": "incremental",
    "unique_key": "deployment_merge_request_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'deployment_merge_requests') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY deployment_merge_request_id ORDER BY _uploaded_at DESC) = 1
