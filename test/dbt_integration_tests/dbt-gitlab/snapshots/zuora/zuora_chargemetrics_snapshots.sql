{% snapshot zuora_chargemetrics_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_query_api', 'chargemetrics') }}
    
{% endsnapshot %}
