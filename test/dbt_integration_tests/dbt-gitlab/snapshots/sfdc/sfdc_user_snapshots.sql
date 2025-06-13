{% snapshot sfdc_user_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols='all',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT *
    FROM {{ source('salesforce', 'user') }}
    
{% endsnapshot %}
