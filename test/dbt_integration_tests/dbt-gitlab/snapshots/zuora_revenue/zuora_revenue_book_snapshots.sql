{% snapshot zuora_revenue_book_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_book') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
