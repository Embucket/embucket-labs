{% snapshot customers_db_orders_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source AS (

      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS orders_rank_in_key
      FROM {{ source('customers', 'customers_db_orders') }}
    )

    SELECT *
    FROM source
    WHERE orders_rank_in_key = 1

{% endsnapshot %}
