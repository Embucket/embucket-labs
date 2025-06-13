WITH final AS (
SELECT *
FROM {{ref('sheetload_sdr_bdr_metric_targets_source')}}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-06-16",
    updated_date="2022-06-16"
) }}
