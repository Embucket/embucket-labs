{% snapshot fct_available_to_renew_snapshot %}
    -- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='primary_key',
          strategy='timestamp',
          updated_at='dbt_created_at',
          invalidate_hard_deletes=True

         )
    }}

    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_available_to_renew'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('fct_available_to_renew') }}

{% endsnapshot %}