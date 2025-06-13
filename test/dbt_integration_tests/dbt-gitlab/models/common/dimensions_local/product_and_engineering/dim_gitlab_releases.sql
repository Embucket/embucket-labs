WITH base AS (

    SELECT *
    FROM {{ ref('releases_source')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY major_minor_version ORDER BY snapshot_date DESC, rank DESC) = 1

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@mpeychet",
    updated_by="@michellecooper",
    created_date="2021-05-03",
    updated_date="2024-03-07"
) }}
