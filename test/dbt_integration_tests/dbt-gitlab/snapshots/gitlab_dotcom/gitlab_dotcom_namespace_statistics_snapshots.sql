{% snapshot gitlab_dotcom_namespace_statistics_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols=[
              'shared_runners_seconds', 
              'shared_runners_seconds_last_reset'
          ],
        )
    }}
    
    SELECT *
    FROM {{ source('gitlab_dotcom', 'namespace_statistics') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    
{% endsnapshot %}
