{% snapshot netsuite_budget_snapshots %}

    {{
        config(
          strategy='check',
          unique_key='budget_id',
          check_cols = ['accounting_book_id',
                        'accounting_period_id',
                        'account_id',
                        'amount',
                        'budget_date',
                        'category_id',
                        'class_id',
                        'customer_id',
                        'date_deleted',
                        'department_id',
                        'item_id',
                        'location_id',
                        'subsidiary_id',
                        ],
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'budget') }}

{% endsnapshot %}
