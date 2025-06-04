{{ config({
    "materialized": "incremental",
    "unique_key": "pipeline_id",
    "on_schema_change": "sync_all_columns"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_pipeline_metadata_internal_only') }}
{% if is_incremental() %}

  WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{ this }})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY _uploaded_at DESC) = 1
