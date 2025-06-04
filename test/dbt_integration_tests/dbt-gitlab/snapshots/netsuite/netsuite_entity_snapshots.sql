{% snapshot netsuite_entity_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='entity_id',
          updated_at='last_modified_date',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'entity') }}

{% endsnapshot %}
