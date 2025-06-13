{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    on_schema_change='sync_all_columns',
    tags=["mnpi_exception", "product"]
  )
}}

WITH flattened AS (

  SELECT
    fct_behavior_structured_event.behavior_structured_event_pk,
    fct_behavior_structured_event.behavior_at,
    fct_behavior_structured_event.ide_extension_version_context,
    fct_behavior_structured_event.extension_name,
    fct_behavior_structured_event.extension_version,
    fct_behavior_structured_event.ide_name,
    fct_behavior_structured_event.ide_vendor,
    fct_behavior_structured_event.ide_version,
    fct_behavior_structured_event.language_server_version
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2023-08-01' -- no events added to context before Aug 2023
    AND has_ide_extension_version_context = TRUE
    {% if is_incremental() %}

        AND fct_behavior_structured_event.behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="flattened",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-04-09",
    updated_date="2024-04-09"
) }}
