{% snapshot sfdc_zqu_quote_amendment_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols='all',
          invalidate_hard_deletes=True
        )
    }}

    SELECT *
    FROM {{ source('salesforce', 'zqu_quote_amendment') }}

{% endsnapshot %}