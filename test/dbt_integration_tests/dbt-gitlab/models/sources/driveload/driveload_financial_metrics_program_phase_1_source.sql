WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'financial_metrics_program_phase_1') }}

), renamed AS (

    SELECT
      arr_month::DATE                                     arr_month,
      fiscal_quarter_name_fy::VARCHAR                     fiscal_quarter_name_fy,
      fiscal_year::NUMBER                                 fiscal_year,
      subscription_start_month::TIMESTAMP_TZ              subscription_start_month,
      subscription_end_month::TIMESTAMP_TZ                subscription_end_month,
      zuora_account_id::VARCHAR                           zuora_account_id,
      zuora_sold_to_country::VARCHAR                      zuora_sold_to_country,
      zuora_account_name::VARCHAR                         zuora_account_name,
      zuora_account_number::VARCHAR                       zuora_account_number,
      dim_crm_account_id::VARCHAR                         dim_crm_account_id,
      dim_parent_crm_account_id::VARCHAR                  dim_parent_crm_account_id,
      parent_crm_account_name::VARCHAR                    parent_crm_account_name,
      parent_crm_account_billing_country::VARCHAR         parent_crm_account_billing_country,
      parent_crm_account_sales_segment::VARCHAR           parent_crm_account_sales_segment,
      parent_crm_account_industry::VARCHAR                parent_crm_account_industry,
      parent_crm_account_owner_team::VARCHAR              parent_crm_account_owner_team,
      parent_crm_account_sales_territory::VARCHAR         parent_crm_account_sales_territory,
      subscription_name::VARCHAR                          subscription_name,
      subscription_status::VARCHAR                        subscription_status,
      subscription_sales_type::VARCHAR                    subscription_sales_type,
      product_category::VARCHAR                           product_category,
      delivery::VARCHAR                                   delivery,
      service_type::VARCHAR                               service_type,
      unit_of_measure::ARRAY                              unit_of_measure,
      mrr::FLOAT                                          mrr,
      arr::FLOAT                                          arr,
      quantity::FLOAT                                     quantity,
      parent_account_cohort_month::DATE                   parent_account_cohort_month,
      months_since_parent_account_cohort_start::NUMBER    months_since_parent_account_cohort_start,
      parent_crm_account_employee_count_band::VARCHAR     parent_crm_account_employee_count_band,
      arr_band_calc::VARCHAR                              arr_band_calc,
      product_name::VARCHAR                               product_name
    FROM source

)

SELECT *
FROM renamed
