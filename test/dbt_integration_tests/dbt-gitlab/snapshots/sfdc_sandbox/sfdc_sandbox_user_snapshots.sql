{% snapshot sfdc_sandbox_user_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce_sandbox', 'user') }}
    
{% endsnapshot %}
