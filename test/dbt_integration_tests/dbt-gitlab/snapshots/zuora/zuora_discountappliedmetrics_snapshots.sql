{% snapshot zuora_discountappliedmetrics_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora', 'discount_applied_metrics') }}
    
{% endsnapshot %}
