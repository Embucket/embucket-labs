{% snapshot customers_db_leads_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True
        )
    }}

    WITH source AS (

      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS leads_rank_in_key
      FROM {{ source('customers', 'customers_db_leads') }}

    )

    SELECT *
    FROM source
    WHERE leads_rank_in_key = 1

{% endsnapshot %}
