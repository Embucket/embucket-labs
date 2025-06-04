{% snapshot sfdc_contact_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols='all',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'contact') }}
    
{% endsnapshot %}
