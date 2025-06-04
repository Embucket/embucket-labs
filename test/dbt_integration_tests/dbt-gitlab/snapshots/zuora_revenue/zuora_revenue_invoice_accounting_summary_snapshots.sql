{% snapshot zuora_revenue_invoice_accounting_summary_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='revenue_snapshot_id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT
        prd_id || '-' || line_id || '-' || root_line_id || '-' || rc_id || '-' || acct_type_id || '-' || acctg_segs|| '-' || schd_type AS revenue_snapshot_id,
        *
    FROM {{ source('zuora_revenue','zuora_revenue_invoice_accounting_summary') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY revenue_snapshot_id ORDER BY updt_dt DESC) = 1

{% endsnapshot %}