{% snapshot zuora_revenue_revenue_contract_hold_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='rc_hold_id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_hold') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rc_hold_id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
