WITH environment AS (


    SELECT
      1 AS dim_environment_id,
      'Gitlab.com' AS environment

    UNION

    SELECT
      2 AS dim_environment_id,
      'License DB' AS environment

    UNION

    SELECT
      3 AS dim_environment_id,
      'Customers Portal' AS environment

)

{{ dbt_audit(
    cte_ref="environment",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-09-22",
    updated_date="2021-09-22"
) }}
