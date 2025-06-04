{% snapshot rpt_ping_metric_totals_w_estimates_monthly_snapshot %}
    -- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='ping_metric_totals_w_estimates_monthly_id',
          strategy='timestamp',
          updated_at='dbt_created_at',
          invalidate_hard_deletes=True
         )
    }}

    SELECT
    {{
          dbt_utils.star(
            from=ref('rpt_ping_metric_totals_w_estimates_monthly'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('rpt_ping_metric_totals_w_estimates_monthly') }}

{% endsnapshot %}
