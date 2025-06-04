{{ config(
    tags=["mnpi_exception"]
) }}

WITH performance_obligation_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_performance_obligation_source') }}

), final AS (

    SELECT

      -- ids
      revenue_contract_performance_obligation_id    AS dim_revenue_contract_performance_obligation_id,
      revenue_contract_performance_obligation_name,

      -- dates
      revenue_contract_performance_obligation_start_date,
      revenue_contract_performance_obligation_end_date,

      -- revenue_contract_performance_obligation details
      revenue_recognition_type,
      revenue_timing,
      release_action_type,
      process_type,
      rule_identifier,
      expiry_field_name,
      expiry_number,
      expiry_number_type,
      retain_method,
      tolerance_percent,
      natural_account,
      revenue_amortization_duration,

      -- dates
      revenue_start_date,
      revenue_end_date,
      release_base_date,
      expiry_date,

      -- accounting segments
      revenue_accounting_segment,
      carve_in_accounting_segment,
      carve_out_accounting_segment,
      contract_liability_accounting_segment,
      contract_asset_accounting_segment,

      -- flags
      is_quantity_distinct,
      is_term_distinct,
      is_apply_manually,
      is_release_manually,
      is_revenue_leading,
      is_carve_in_leading,
      is_carve_out_leading,
      is_contract_liability_leading,
      is_contract_asset_leading,
      is_performance_obligation_template_dependency,
      is_latest_version,
      is_consumption,
      is_performance_obligation_satisfied,
      is_distinct,
      is_manual_edit,
      is_cumulative_prcs,
      is_release_immediate,
      is_sales_order_term_change,
      is_manual_rearranged,
      is_manual_release,
      is_performance_obligation_dependency,
      is_performance_obligation_processed,
      is_performance_obligation_removed,
      is_performance_obligation_multiple_sign,
      is_performance_obligation_removal,
      is_performance_obligation_manual,
      is_performance_obligation_orphan,
      is_performance_obligation_manual_forecast,

      -- pob details
      performance_obligation_version,

      -- pob template details
      performance_obligation_template_name,
      performance_obligation_template_description,
      performance_obligation_template_version,
      
      -- meta data
      performance_obligation_template_created_by
      performance_obligation_template_created_date,
      performance_obligation_template_updated_by,
      performance_obligation_template_updated_date,

      -- metadata
      revenue_contract_performance_obligation_created_by,
      revenue_contract_performance_obligation_created_date,
      revenue_contract_performance_obligation_updated_by,
      revenue_contract_performance_obligation_updated_date,
      security_attribute_value

    FROM performance_obligation_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}