{{ config(
    tags=["mnpi_exception"]
) }}

WITH lines_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_line_source') }}

), final AS (

    SELECT 

      -- ids
      revenue_contract_line_id          AS dim_revenue_contract_line_id,
      revenue_contract_line_type,
      
      -- attributes
      customer_name,
      transactional_currency,
      functional_currency,
      fair_value_type,
      error_message,
      sales_representative_name,
      customer_number,
      product_category,
      product_class,
      product_family,
      product_line,
      business_unit,
      contract_date,
      revenue_contract_line_comment,
      approval_status,
      rssp_calculation_type,
      delink_level,
      impairment_type,
      revenue_contract_level,
      product_life_term,
      sequence_number,
      average_pricing_method,
      percent_format,
      reason_code,
      contract_modification_code,
      action_type,
      step_1_revenue_contract_level_range,
      price_point,

      -- dates
      contract_modification_date,
      revenue_start_date,
      revenue_end_date,
      scheduled_ship_date,
      ship_date,
      incremental_update_date,
      fair_value_date,
      original_fair_value_date,
      forecast_date,
      company_code,

      --segments
      offset_accounting_segment,
      deferred_accounting_segment,
      revenue_accounting_segment,

      -- accounts
      intercompany_account,
      contract_asset_account,
      ci_account,
      al_account,
      ar_account,
      contra_ar_account,
      payables_account,
      long_term_deferred_adjustment_account,
      ub_liability_account,
      long_term_deferred_cogs_account,
      long_term_ca_account,

      -- metadata
      revenue_contract_line_created_by,
      revenue_contract_line_created_date,
      revenue_contract_line_updated_by,
      revenue_contract_line_updated_date

    FROM lines_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
 	) 
 }}
