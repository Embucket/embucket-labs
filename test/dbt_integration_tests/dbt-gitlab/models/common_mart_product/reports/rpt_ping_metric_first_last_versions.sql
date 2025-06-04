{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


{{ simple_cte([
    ('dim_gitlab_releases', 'dim_gitlab_releases'),
    ('dim_ping_metric', 'dim_ping_metric'),
    ('dim_ping_instance', 'dim_ping_instance')
    ])
}}

,
aggregation AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['metrics_path', 'ping_edition', 'version_is_prerelease']) }} AS ping_metric_first_last_versions_id,
    dim_ping_metric.metrics_path,
    dim_ping_instance.ping_edition,
    dim_ping_instance.version_is_prerelease,
    ARRAY_SORT(ARRAY_UNIQUE_AGG(dim_ping_instance.major_minor_version_num))                           AS version_numbers,
    GET(version_numbers, 0)                                                                           AS first_major_minor_version_id_with_counter,
    TRUNC(first_major_minor_version_id_with_counter / 100, 0)                                         AS first_major_version_with_counter,
    (first_major_minor_version_id_with_counter - (first_major_version_with_counter * 100))            AS first_minor_version_with_counter,
    CAST (first_major_version_with_counter AS VARCHAR) || '.'
    || CAST (first_minor_version_with_counter AS VARCHAR)                                             AS first_major_minor_version_with_counter,
    GET(version_numbers, ARRAY_SIZE(version_numbers) - 1)                                             AS last_major_minor_version_id_with_counter,
    TRUNC(last_major_minor_version_id_with_counter / 100, 0)                                          AS last_major_version_with_counter,
    (last_major_minor_version_id_with_counter - (last_major_version_with_counter * 100))              AS last_minor_version_with_counter,
    CAST (last_major_version_with_counter AS VARCHAR) || '.'
    || CAST (last_minor_version_with_counter AS VARCHAR)                                              AS last_major_minor_version_with_counter,
    HLL(dim_ping_instance.dim_installation_id)                                                        AS dim_installation_count
  FROM dim_ping_metric
  INNER JOIN dim_ping_instance
    ON GET_PATH(dim_ping_instance.raw_usage_data_payload, dim_ping_metric.metrics_path) IS NOT NULL
  INNER JOIN dim_gitlab_releases --limit to valid versions
    ON dim_ping_instance.major_minor_version = dim_gitlab_releases.major_minor_version
  WHERE dim_ping_instance.dim_instance_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' -- Removing SaaS
  GROUP BY 1, 2, 3, 4
)

{{ dbt_audit(
    cte_ref="aggregation",
    created_by="@icooper-acp",
    updated_by="@pempey",
    created_date="2022-04-07",
    updated_date="2024-12-27"
) }}
