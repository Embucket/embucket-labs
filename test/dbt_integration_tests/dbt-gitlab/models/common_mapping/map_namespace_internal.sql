{{ config({
        "materialized": "view",
    })
}}

{{ simple_cte([
    ('prep_namespace', 'prep_namespace')
]) }},

final AS (

  SELECT DISTINCT 
    ultimate_parent_namespace_id
  FROM prep_namespace
  WHERE namespace_is_internal = TRUE

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2020-12-29",
    updated_date="2024-11-15"
) }}
