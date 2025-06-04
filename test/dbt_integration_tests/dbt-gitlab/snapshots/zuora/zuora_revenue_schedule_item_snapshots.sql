{% snapshot zuora_revenue_schedule_item_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}

    SELECT *
    FROM {{ source('zuora', 'revenue_schedule_item') }}

{% endsnapshot %}
