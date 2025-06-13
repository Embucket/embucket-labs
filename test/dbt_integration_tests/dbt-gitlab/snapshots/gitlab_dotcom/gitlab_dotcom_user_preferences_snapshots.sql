{% snapshot gitlab_dotcom_user_preferences_snapshots %}

    {{
        config(
          unique_key='user_id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'user_preferences') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1)

{% endsnapshot %}
