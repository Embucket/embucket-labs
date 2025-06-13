{% snapshot tableau_cloud_groups_snapshot %}

{{
    config(
      unique_key='user_group_sk',
      strategy='check',
      check_cols=['"Group LUID"', '"User LUID"'],
      invalidate_hard_deletes=True,
      post_hook=["{{ rolling_window_delete('UPLOADED_AT', 'month', 24) }}"]
    )
}}

WITH snapshot_data AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['"Group LUID"', '"User LUID"']) }} AS user_group_sk,
    *
  FROM {{ source('tableau_cloud', 'groups') }}
)

SELECT *
FROM snapshot_data

{% endsnapshot %}