{% snapshot gitlab_dotcom_user_details_snapshots %}

    {{
        config(
          unique_key='user_id',
          strategy='timestamp',
          updated_at='uploaded_at',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *, TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT)) as uploaded_at
    FROM {{ source('gitlab_dotcom', 'user_details') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY uploaded_at DESC) = 1)

{% endsnapshot %}
