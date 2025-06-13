{% snapshot customers_db_customers_snapshots %}

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
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS customers_rank_in_key
        
      FROM {{ source('customers', 'customers_db_customers') }}

    )

    SELECT *
    FROM source
    WHERE customers_rank_in_key = 1
    
{% endsnapshot %}
