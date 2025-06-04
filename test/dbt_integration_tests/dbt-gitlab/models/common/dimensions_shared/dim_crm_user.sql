-- depends_on: {{ ref('prep_crm_user') }}
{{ config(
    tags=["six_hourly"]
) }}

{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_user_id') }}"
    })
}}


WITH final AS (

    SELECT 
      {{ dbt_utils.star(
           from=ref('prep_crm_user'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
    FROM {{ ref('prep_crm_user') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@jonglee1218",
    created_date="2020-11-20",
    updated_date="2025-02-06"
) }}
