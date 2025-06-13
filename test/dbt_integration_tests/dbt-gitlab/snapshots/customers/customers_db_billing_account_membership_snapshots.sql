{% snapshot customers_db_billing_account_memberships_snapshots %}

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
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS billing_account_membership_rank_in_key
      FROM {{ source('customers', 'customers_db_billing_account_memberships') }}
    )

    SELECT *
    FROM source
    WHERE billing_account_membership_rank_in_key = 1

{% endsnapshot %}
