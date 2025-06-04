{% snapshot sfdc_bizible_attribution_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols='all',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'bizible_attribution_touchpoint') }}
    
{% endsnapshot %}
