{{ config(
    tags=["mnpi_exception"]
) }}

WITH sfdc_opportunity_source AS (

    SELECT *
    FROM {{ ref('prep_crm_opportunity') }}
    WHERE is_live
      AND channel_type IS NOT NULL

), final AS (

    SELECT DISTINCT
      {{ dbt_utils.generate_surrogate_key(['channel_type']) }}   AS dim_channel_type_id,
      channel_type                                      AS channel_type_name
    FROM sfdc_opportunity_source

    UNION ALL

    SELECT
      MD5('-1')                                         AS dim_channel_type_id,
      'Missing channel_type_name'                       AS channel_type_name

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-04-07",
    updated_date="2021-04-28"
) }}
