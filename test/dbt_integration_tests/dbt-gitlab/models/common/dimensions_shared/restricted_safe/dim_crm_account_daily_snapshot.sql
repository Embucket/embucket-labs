-- depends_on: {{ ref('driveload_lam_corrections_source') }}
{{ config({
        "materialized": "incremental",
        "unique_key": "crm_account_snapshot_id",
        "tags": ["account_snapshots"],
    })
}}

WITH final AS (

    SELECT 
      {{ dbt_utils.star(
           from=ref('prep_crm_account_daily_snapshot'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('prep_crm_account_daily_snapshot') }}
    {% if is_incremental() %}

    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jonglee1218",
    created_date="2022-01-25",
    updated_date="2025-02-06"
) }}