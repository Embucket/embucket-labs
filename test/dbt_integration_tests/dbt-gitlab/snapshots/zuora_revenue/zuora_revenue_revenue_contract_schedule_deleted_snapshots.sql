{% snapshot zuora_revenue_revenue_contract_schedule_deleted_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='schd_id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_schedule_deleted') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY schd_id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
