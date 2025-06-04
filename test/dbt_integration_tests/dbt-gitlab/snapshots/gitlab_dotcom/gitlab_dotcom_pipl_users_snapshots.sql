{% snapshot gitlab_dotcom_pipl_users_snapshots %}

    {{
        config(
          unique_key='user_id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'pipl_users') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1)

{% endsnapshot %}
