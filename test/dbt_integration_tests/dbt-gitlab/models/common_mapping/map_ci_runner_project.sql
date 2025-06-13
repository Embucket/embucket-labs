{{ simple_cte([
    ('source', 'gitlab_dotcom_ci_runner_projects_source'),
    ('dim_date', 'dim_date'),
]) }}

, renamed AS (

    SELECT 
      ci_runner_project_id AS dim_ci_runner_project_id,
      runner_id            AS dim_ci_runner_id,
      project_id           AS dim_project_id,
      date_id              AS dim_date_id,
      created_at,
      updated_at
    FROM source
    LEFT JOIN dim_date ON TO_DATE(created_at) = dim_date.date_day
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-31",
    updated_date="2021-05-31"
) }}
