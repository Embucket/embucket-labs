{% snapshot fct_mrr_snapshot %}
-- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='mrr_id',
          strategy='check',
          check_cols = ['dim_charge_id','dim_product_detail_id','dim_subscription_id','dim_billing_account_id','dim_crm_account_id','dim_order_id','subscription_status','unit_of_measure','mrr','arr','quantity'],
          invalidate_hard_deletes=True
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_mrr'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('fct_mrr') }}

{% endsnapshot %}
