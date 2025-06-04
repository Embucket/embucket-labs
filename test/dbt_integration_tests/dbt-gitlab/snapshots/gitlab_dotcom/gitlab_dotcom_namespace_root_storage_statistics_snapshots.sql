{% snapshot gitlab_dotcom_namespace_root_storage_statistics_snapshots %}

    {{
        config(
          unique_key='namespace_id',
          strategy='check',
          check_cols=[
              'repository_size', 
              'lfs_objects_size',
              'wiki_size',
              'build_artifacts_size',
              'storage_size',
              'packages_size',
              'container_registry_size'
          ],
        )
    }}
    
    SELECT *       
    FROM {{ source('gitlab_dotcom', 'namespace_root_storage_statistics') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY _uploaded_at DESC) = 1
    
{% endsnapshot %}
