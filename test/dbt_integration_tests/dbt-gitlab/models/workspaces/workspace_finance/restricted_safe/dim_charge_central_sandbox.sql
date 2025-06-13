WITH prep_charge AS (

    SELECT *
    FROM {{ ref('prep_charge_central_sandbox') }}

), final AS (

    SELECT
      --Surrogate Key
      prep_charge.dim_charge_id,

      --Natural Key
      prep_charge.subscription_name,
      prep_charge.subscription_version,
      prep_charge.rate_plan_charge_number,
      prep_charge.rate_plan_charge_version,
      prep_charge.rate_plan_charge_segment,

      --Dates
      prep_charge.effective_start_date,
      prep_charge.effective_end_date,
      prep_charge.effective_start_month,
      prep_charge.effective_end_month,
      prep_charge.charged_through_date,
      prep_charge.charge_created_date,
      prep_charge.charge_updated_date,
      prep_charge.charge_term,

      --ARR Analysis Framework
      prep_charge.type_of_arr_change,

      --Information
      prep_charge.rate_plan_name,
      prep_charge.rate_plan_charge_name,
      prep_charge.rate_plan_charge_description,
      prep_charge.is_last_segment,
      prep_charge.discount_level,
      prep_charge.charge_type,
      prep_charge.rate_plan_charge_amendment_type,
      prep_charge.unit_of_measure,
      prep_charge.is_paid_in_full,
      prep_charge.is_included_in_arr_calc,
      prep_charge.months_of_future_billings

    FROM prep_charge

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-04-13",
    updated_date="2024-10-09"
) }}
