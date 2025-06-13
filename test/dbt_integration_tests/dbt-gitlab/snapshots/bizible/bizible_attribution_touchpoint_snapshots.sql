{% snapshot bizible_attribution_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='_modified_date',
        )
    }}
    
    SELECT * 
    FROM {{ source('bizible', 'biz_attribution_touchpoints') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY uploaded_at DESC) = 1
    
{% endsnapshot %}
