WITH base AS (

    SELECT *
    FROM {{ ref('driveload_ssa_quarterly_aggregated_metrics_for_coverage_source') }}

)

SELECT *
FROM base
