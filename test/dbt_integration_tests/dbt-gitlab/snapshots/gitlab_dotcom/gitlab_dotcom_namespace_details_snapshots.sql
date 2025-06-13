{% snapshot gitlab_dotcom_namespace_details_snapshots %}

    {{
        config(
          unique_key='namespace_id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT *
    FROM {{ source('gitlab_dotcom', 'namespace_details') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY updated_at DESC) = 1)

{% endsnapshot %}
