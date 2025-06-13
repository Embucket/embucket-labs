{% snapshot netsuite_classes_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='class_id',
          updated_at='date_last_modified',
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'classes') }}

{% endsnapshot %}
