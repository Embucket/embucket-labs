{{ config(
    tags=["six_hourly"]
) }}

{{ sfdc_user_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@jonglee1218",
    created_date="2021-01-12",
    updated_date="2025-02-06"
) }}
