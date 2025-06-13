{% snapshot zuora_rateplan_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora', 'rate_plan') }}
    
{% endsnapshot %}
