{% snapshot netsuite_subsidiaries_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='subsidiary_id',
          updated_at='date_last_modified',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'subsidiaries') }}

{% endsnapshot %}
