{{ config(
    tags=["mnpi_exception"]
) }}

WITH quote AS (

    SELECT
      dim_quote_id,
      quote_number,
      quote_name,
      quote_status,
      quote_entity,
      subscription_action_type,
      is_education_exemption_comment,
      is_education_approval_required,
      is_primary_quote,
      quote_start_date,
      created_date
    FROM {{ ref('prep_quote') }}
    WHERE dim_quote_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="quote",
    created_by="@snalamaru",
    updated_by="@rkohnke",
    created_date="2021-01-07",
    updated_date="2024-10-31"
) }}
