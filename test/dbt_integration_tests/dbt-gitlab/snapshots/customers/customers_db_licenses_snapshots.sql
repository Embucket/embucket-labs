{% snapshot customers_db_licenses_snapshots %}

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
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS license_rank_in_key
      FROM {{ source('customers', 'customers_db_licenses') }}

    )

    SELECT 
      {{ dbt_utils.star(from=source('customers', 'customers_db_licenses'), except=["CREATED_AT", "UPDATED_AT"]) }},
      TO_TIMESTAMP_NTZ(created_at) AS created_at,
      TO_TIMESTAMP_NTZ(updated_at) AS updated_at
    FROM source
    WHERE license_rank_in_key = 1
    
{% endsnapshot %}
