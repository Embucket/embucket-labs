{% snapshot sheetload_gitlab_roulette_capacity_history_snapshot %}

{{
    config(
      unique_key='unique_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

  SELECT
    {{ dbt_utils.generate_surrogate_key(['project','role','timestamp']) }} AS unique_id,
    *,
    _updated_at::NUMBER::TIMESTAMP AS updated_at
  FROM {{ source('sheetload','gitlab_roulette_capacity_history') }}

{% endsnapshot %}
