{% snapshot tableau_cloud_users_snapshot %}

{{
    config(
      unique_key='"User ID"',
      strategy='timestamp',
      updated_at='uploaded_at',
      invalidate_hard_deletes=True,
      post_hook=["{{ rolling_window_delete('uploaded_at','month',24) }}"]
    )
}}

WITH snapshot_data AS (
  SELECT
    *
  FROM {{ source('tableau_cloud', 'users') }}
)

SELECT *
FROM snapshot_data

{% endsnapshot %}
