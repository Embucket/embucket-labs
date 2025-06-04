{% snapshot gitlab_dotcom_members_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='created_at',
        )
    }}
    
    SELECT *
    FROM {{ source('gitlab_dotcom', 'members') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1)

{% endsnapshot %}
