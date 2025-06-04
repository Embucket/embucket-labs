{{ config(
    tags=["mnpi_exception"]
) }}

WITH channel_type AS (

    SELECT
      dim_channel_type_id,
      channel_type_name
    FROM {{ ref('prep_channel_type') }}
)

{{ dbt_audit(
    cte_ref="channel_type",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-04-28",
    updated_date="2021-04-28"
) }}
