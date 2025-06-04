{% snapshot sfdc_zqu_product_rate_plan_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols='all',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('salesforce', 'zqu_product_rate_plan') }}

{% endsnapshot %}
