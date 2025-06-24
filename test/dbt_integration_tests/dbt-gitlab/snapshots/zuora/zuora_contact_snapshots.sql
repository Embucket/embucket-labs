{% snapshot zuora_contact_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora', 'dup_zuora_stitch_contact') }}
    
{% endsnapshot %}
