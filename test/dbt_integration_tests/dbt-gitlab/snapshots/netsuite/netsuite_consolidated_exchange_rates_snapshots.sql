{% snapshot netsuite_consolidated_exchange_rates_snapshots %}

    {{
        config(
          strategy='check',
          unique_key='consolidated_exchange_rate_id',
          check_cols = ['accounting_book_id',
                        'accounting_period_id',
                        'average_budget_rate',
                        'average_rate',
                        'current_budget_rate',
                        'current_rate',
                        'date_deleted',
                        'from_subsidiary_id',
                        'historical_budget_rate',
                        'historical_rate',
                        'to_subsidiary_id',
                        ]
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'consolidated_exchange_rates') }}

{% endsnapshot %}
