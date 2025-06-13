{% snapshot gitlab_dotcom_subscription_add_on_purchases_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('gitlab_dotcom', 'subscription_add_on_purchases') }}
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1)

{% endsnapshot %}
