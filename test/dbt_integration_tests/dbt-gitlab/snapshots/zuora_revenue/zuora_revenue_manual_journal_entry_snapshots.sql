{% snapshot zuora_revenue_manual_journal_entry_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='je_line_id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_manual_journal_entry') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY je_line_id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
