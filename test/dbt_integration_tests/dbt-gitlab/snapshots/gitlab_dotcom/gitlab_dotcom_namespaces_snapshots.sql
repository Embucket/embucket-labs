{% snapshot gitlab_dotcom_namespaces_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    SELECT * 
    FROM {{ source('gitlab_dotcom', 'namespaces') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1)
    
{% endsnapshot %}
