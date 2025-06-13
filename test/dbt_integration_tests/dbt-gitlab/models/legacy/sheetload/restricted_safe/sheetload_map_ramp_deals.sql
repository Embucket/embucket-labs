WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_map_ramp_deals_source') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-06-16",
    updated_date="2022-06-16"
) }}