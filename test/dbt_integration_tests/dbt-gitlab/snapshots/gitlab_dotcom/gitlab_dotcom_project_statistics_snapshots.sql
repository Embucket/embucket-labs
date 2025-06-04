{% snapshot gitlab_dotcom_project_statistics_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols=[
              'repository_size', 
              'commit_count', 
              'storage_size', 
              'lfs_objects_size',
              'packages_size',
              'wiki_size',
              'build_artifacts_size', 
              'container_registry_size',
              'shared_runners_seconds', 
              'shared_runners_seconds_last_reset',
          ],
        )
    }}
    
    SELECT *       
    FROM {{ source('gitlab_dotcom', 'project_statistics') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
    
{% endsnapshot %}
