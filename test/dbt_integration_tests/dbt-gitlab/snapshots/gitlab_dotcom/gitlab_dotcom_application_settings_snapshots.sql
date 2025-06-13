{% snapshot gitlab_dotcom_application_settings_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols=[
              'shared_runners_minutes',
              'repository_size_limit'
          ],
        )
    }}
    
    SELECT *
    FROM {{ source('gitlab_dotcom', 'application_settings') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    
{% endsnapshot %}
