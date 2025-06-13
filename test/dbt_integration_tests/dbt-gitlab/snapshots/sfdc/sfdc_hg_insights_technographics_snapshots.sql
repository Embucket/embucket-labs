{% snapshot sfdc_hg_insights_technographics_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='systemmodstamp',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT * 
    FROM {{ source('salesforce', 'hg_insights_technographics') }}
    
{% endsnapshot %}