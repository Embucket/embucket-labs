WITH lines_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_line_source') }}

), pob AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_performance_obligation_source') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('date_details_source') }}

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), prep_quote AS (

    SELECT *
    FROM {{ ref('prep_quote') }}

), final AS (

    SELECT 

      -- ids
      lines_source.revenue_contract_line_id                                             AS dim_revenue_contract_line_id,
      lines_source.revenue_contract_id                                                  AS dim_revenue_contract_id,
      lines_source.revenue_contract_batch_id,

      -- pob ids
      lines_source.revenue_contract_performance_obligation_id                           AS dim_revenue_contract_performance_obligation_id,
      pob.event_id  											                        AS dim_accounting_event_id_performance_obligation,

      -- account ids
      lines_source.billing_account_id 							                        AS dim_billing_account_id,
      zuora_account.crm_id 										                        AS dim_crm_account_id,

      -- order ids
      lines_source.sales_order_number,
      lines_source.sales_order_line_id,
      lines_source.sales_order_line_number,

      -- subscription ids
      lines_source.subscription_id 								                        AS dim_subscription_id,
      lines_source.amendment_id  								                        AS dim_amendment_id,


      -- purchase order ids
      lines_source.purchase_order_number,
      prep_quote.dim_quote_id,

      -- contract ids
      lines_source.contract_number,
      lines_source.contract_line_number,
      lines_source.contract_line_id,

      -- order ids
      lines_source.order_id,
      lines_source.order_item_id,
      lines_source.order_action_id,


      -- product ids
      lines_source.product_id,
      lines_source.rate_plan_id,
      lines_source.rate_plan_charge_id,
      lines_source.original_rate_plan_charge_id,
      lines_source.product_rate_plan_id,
      lines_source.product_rate_plan_charge_id 					                        AS dim_product_detail_id,
      lines_source.revenue_contract_bill_item_id				                        AS dim_invoice_item_id,
      lines_source.zbilling_batch_id,
      lines_source.ramp_deal_id,
      lines_source.k2_batch_id,
      lines_source.ramp_id,

      -- miscellaneous ids
      lines_source.set_of_books_id,
      lines_source.bundle_configuration_id,
      lines_source.variable_consideration_type_id,
      lines_source.bundle_parent_id,
      lines_source.fair_value_group_id,
      lines_source.fair_value_template_id,
      lines_source.material_rights_line_id,
      lines_source.reference_document_line_id,
      lines_source.split_reference_document_line_id,

      -- dates
      {{ get_date_id('lines_source.sales_order_book_date') }}                           AS sales_order_book_date_id,
      {{ get_date_id('lines_source.revenue_start_date') }}                              AS revenue_start_date_id,
      {{ get_date_id('lines_source.revenue_end_date') }}                                AS revenue_end_date_id,
      {{ get_date_id('lines_source.scheduled_ship_date') }}                             AS scheduled_ship_date_id,
      {{ get_date_id('lines_source.ship_date') }}                                       AS ship_date_id,
      {{ get_date_id('lines_source.contract_modification_date') }}                      AS contract_modification_date_id,
      {{ get_date_id('lines_source.contract_date') }}                                   AS contract_date_id,
      {{ get_date_id('lines_source.fair_value_date') }}                                 AS fair_value_date_id,
      {{ get_date_id('lines_source.original_fair_value_date') }}                        AS original_fair_value_date_id,
      {{ get_date_id('lines_source.fair_value_expiration') }}                           AS fair_value_expiration_id,
      {{ get_date_id('lines_source.forecast_date') }}                                   AS forecast_date_id,
      {{ get_date_id('lines_source.unbilled_reversal_period') }}                        AS unbilled_reversal_period_date_id,
      {{ get_date_id('lines_source.deferred_period_id') }}                              AS deferred_period_date_id,
      {{ get_date_id('pob.revenue_contract_performance_obligation_created_date') }}     AS revenue_contract_performance_obligation_created_date_id,
      {{ get_date_id('pob.revenue_contract_performance_obligation_updated_date') }}     AS revenue_contract_performance_obligation_updated_date_id,

      -- attributes
      pob.revenue_contract_performance_obligation_name,
      lines_source.revenue_contract_line_term,
      lines_source.revenue_amortization_duration,
      lines_source.product_life_term,
      lines_source.link_identifier,
      lines_source.ramp_identifier,

      -- price details
      lines_source.unit_sell_price,
      lines_source.ssp_sell_price,
      lines_source.old_sell_price,
      lines_source.net_sell_price,
      lines_source.extended_selling_price,
      lines_source.list_price,
      lines_source.unit_list_price,
      lines_source.ssp_list_price,
      lines_source.net_list_price,
      lines_source.fair_value_price,
      lines_source.previous_fair_value,
      lines_source.extended_fair_value_price,
      lines_source.allocatable_price,
      lines_source.allocatable_functional_price,
      lines_source.allocated_price,
      lines_source.below_fair_value_price,
      lines_source.above_fair_value_price,
      
      
      -- amounts
      lines_source.deferred_amount,
      lines_source.recognized_amount,
      lines_source.billed_deferred_amount,
      lines_source.billed_recognized_amount,
      lines_source.pord_deferred_amount,
      lines_source.pord_recognized_amount,
      lines_source.carve_amount,
      lines_source.carve_amount_imprtmt,
      lines_source.cumulative_carve_amount,
      lines_source.discount_amount,
      lines_source.cumulative_allocated_amount,
      lines_source.varaiable_consideration_amount,
      lines_source.impairment_retrieve_amount,
      lines_source.overstated_amount,
      lines_source.overstated_list_price_amount,
      lines_source.ramp_carve_amount,
      lines_source.ramp_cumulative_carve_amount,
      lines_source.ramp_cumulative_allocated_amount,
      lines_source.unscheduled_adjustment,

      -- quantities
      lines_source.order_quantity,
      lines_source.invoice_quantity,
      lines_source.return_quantity,
      lines_source.original_quantity,
      
      -- exchange rates
      lines_source.functional_currency_exchage_rate,
      lines_source.reporting_currency_exchange_rate,

      -- percents
      lines_source.discount_percent,
      lines_source.fair_value_percent,
      lines_source.posted_percent,
      lines_source.released_percent,
      lines_source.transaction_price_ssp_percent,
      lines_source.material_rights_org_percent,
      lines_source.ramp_allocation_percent,
      lines_source.ramp_allocatable_percent,
      lines_source.ramp_allocted_percent,
      lines_source.total_budget_hours,
      lines_source.total_budget_cost,

      -- flags
      lines_source.is_carve_eligible,
      lines_source.is_return,
      lines_source.is_within_fair_value_range,
      lines_source.is_stated,
      lines_source.is_standalone,
      lines_source.is_discount_adjustment,
      lines_source.is_fair_value_eligible,
      lines_source.is_manual_fair_value,
      lines_source.is_unbilled,
      lines_source.is_manual_created,
      lines_source.is_variable_consideration_clearing,
      lines_source.is_manual_journal_entry_line,
      lines_source.is_update_or_insert,
      lines_source.is_cancelled,
      lines_source.is_allocation_recognition_hold,
      lines_source.is_allocation_schedule_hold,
      lines_source.is_allocation_treatment,
      lines_source.is_contra_entry,
      lines_source.is_conv_waterfall,
      lines_source.is_reclass,
      lines_source.is_revenue_recognition_hold,
      lines_source.is_revenue_schedule_hold,
      lines_source.is_revevnue_schedule,
      lines_source.is_transfer_hold,
      lines_source.is_allocation_delink,
      lines_source.is_canceled_by_reduction_order,
      lines_source.is_level_2_carve_eligible,
      lines_source.is_rssp_failed,
      lines_source.is_variable_consideration_eligible,
      lines_source.is_ghost_line,
      lines_source.is_initial_contract,
      lines_source.is_material_rights,
      lines_source.is_ramp_up,
      lines_source.is_split,
      lines_source.is_ord_orch,
      lines_source.is_new_performance_obligation,
      lines_source.full_cm_flag,
      lines_source.is_skip_contract_modification,
      lines_source.is_impairment_exception,
      lines_source.is_pros_deferred,
      lines_source.is_manual_sales_order,
      lines_source.is_zero_dollar_recognition,
      lines_source.is_full_pord_discount,
      lines_source.is_zero_dollar_reduction_order,
      lines_source.is_restricted_sales_order_value_update,
      lines_source.is_zbilling_complete,
      lines_source.is_zbilling_unscheduled_adjustment,
      lines_source.is_zbillling_cancelled_line,
      lines_source.is_non_distinct_performance_obligation,
      lines_source.is_zbilling_contract_modification_rule,
      lines_source.is_system_inv_exist,
      lines_source.is_zbillling_manual_sales_order,
      lines_source.is_sales_order_term_change,
      lines_source.is_overage_exists,
      lines_source.is_zbilling_ramp,
      lines_source.is_updated_by_reduction_order,
      lines_source.is_unbilled_evergreen,
      lines_source.is_ramp_carve,
      lines_source.is_zero_f,
      lines_source.is_pros_decrse_p,

      --metadata
      lines_source.security_attribute_value,
      {{ get_date_id('lines_source.revenue_contract_line_created_date') }}                      AS revenue_contract_line_created_date_id,
      {{ get_date_id('lines_source.revenue_contract_line_updated_date') }}                      AS revenue_contract_line_updated_date_id,
      {{ get_date_id('lines_source.incremental_update_date') }}                                 AS incremental_update_date_id,
      pob.revenue_contract_performance_obligation_created_by,
      pob.revenue_contract_performance_obligation_updated_by

    FROM lines_source
    LEFT JOIN pob 
      ON lines_source.revenue_contract_performance_obligation_id = pob.revenue_contract_performance_obligation_id
    LEFT JOIN zuora_account 
      ON lines_source.billing_account_id = zuora_account.account_id
    LEFT JOIN prep_quote
      ON lines_source.quote_number = prep_quote.quote_number

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
 	) 
 }}
