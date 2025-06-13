{% snapshot gitlab_dotcom_users_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'users') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1)

{% endsnapshot %}
