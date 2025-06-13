{% snapshot zuora_revenue_approval_detail_snapshots %}

{{
        config(
          strategy='timestamp',
          unique_key="snapshot_id",
          updated_at='incr_updt_dt',
        )
    }}

  SELECT
    *,
    rc_id || '-' || rc_appr_id || '-' || COALESCE(approval_status, '_') AS snapshot_id
  FROM {{ source('zuora_revenue', 'zuora_revenue_approval_detail') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY rc_appr_id, approver_sequence, approval_rule_id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
