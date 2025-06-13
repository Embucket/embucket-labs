{{ config({
        "materialized": "incremental",
        "unique_key": "crm_user_snapshot_id",
        "tags": ["user_snapshots", "six_hourly"],
    })
}}

{{sfdc_user_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jonglee1218",
    created_date="2023-03-10",
    updated_date="2025-02-06"
) }}
