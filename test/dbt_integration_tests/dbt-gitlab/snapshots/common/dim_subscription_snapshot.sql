{% snapshot dim_subscription_snapshot %}
-- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='dim_subscription_id',
          strategy='timestamp',
          updated_at='dbt_created_at',
          invalidate_hard_deletes=True
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('dim_subscription'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('dim_subscription') }}

{% endsnapshot %}
