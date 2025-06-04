{{ config({
    "materialized": "incremental",
    "unique_key": "issue_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'epic_issues') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY _uploaded_at DESC) = 1
