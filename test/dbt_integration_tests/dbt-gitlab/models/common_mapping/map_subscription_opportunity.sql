{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_subscription_opportunity_mapping AS (

    SELECT *
    FROM {{ ref('prep_subscription_opportunity_mapping') }}

), final_mapping AS (

    SELECT DISTINCT
      dim_subscription_id,
      dim_crm_opportunity_id,
      is_questionable_opportunity_mapping
    FROM prep_subscription_opportunity_mapping
    WHERE dim_crm_opportunity_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final_mapping",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-11-10",
    updated_date="2021-11-16"
) }}
