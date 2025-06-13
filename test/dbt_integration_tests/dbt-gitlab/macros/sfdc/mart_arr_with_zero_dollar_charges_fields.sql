{% macro mart_arr_with_zero_dollar_charges_fields(cte) %}

      --primary_key
      {{ cte }}.primary_key,

      --Foreign Keys
      {{ cte }}.dim_billing_account_id,
      {{ cte }}.dim_crm_account_id,
      {{ cte }}.dim_subscription_id,
      {{ cte }}.dim_product_detail_id,

      --date info
      {{ cte }}.subscription_start_month,
      {{ cte }}.subscription_end_month,

      --billing account info
      {{ cte }}.sold_to_country,
      {{ cte }}.billing_account_name,
      {{ cte }}.billing_account_number,
      {{ cte }}.ssp_channel,
      {{ cte }}.po_required,
      {{ cte }}.auto_pay,
      {{ cte }}.default_payment_method_type,

      -- crm account info
      {{ cte }}.crm_account_name,
      {{ cte }}.dim_parent_crm_account_id,
      {{ cte }}.parent_crm_account_name,
      {{ cte }}.parent_crm_account_sales_segment,
      {{ cte }}.parent_crm_account_industry,
      {{ cte }}.crm_account_employee_count_band,
      {{ cte }}.health_score_color,
      {{ cte }}.health_number,
      {{ cte }}.is_jihu_account,
      {{ cte }}.parent_crm_account_lam,
      {{ cte }}.parent_crm_account_lam_dev_count,
      {{ cte }}.parent_crm_account_business_unit,
      {{ cte }}.parent_crm_account_geo,
      {{ cte }}.parent_crm_account_region,
      {{ cte }}.parent_crm_account_area,
      {{ cte }}.parent_crm_account_role_type,
      {{ cte }}.parent_crm_account_territory,
      {{ cte }}.parent_crm_account_max_family_employee,
      {{ cte }}.parent_crm_account_upa_country,
      {{ cte }}.parent_crm_account_upa_state,
      {{ cte }}.parent_crm_account_upa_city,
      {{ cte }}.parent_crm_account_upa_street,
      {{ cte }}.parent_crm_account_upa_postal_code,
      {{ cte }}.crm_account_employee_count,

      --subscription info
      {{ cte }}.dim_subscription_id_original,
      {{ cte }}.subscription_status,
      {{ cte }}.subscription_sales_type,
      {{ cte }}.subscription_name,
      {{ cte }}.subscription_name_slugify,
      {{ cte }}.oldest_subscription_in_cohort,
      {{ cte }}.subscription_lineage,
      {{ cte }}.subscription_cohort_month,
      {{ cte }}.subscription_cohort_quarter,
      {{ cte }}.billing_account_cohort_month,
      {{ cte }}.billing_account_cohort_quarter,
      {{ cte }}.crm_account_cohort_month,
      {{ cte }}.crm_account_cohort_quarter,
      {{ cte }}.parent_account_cohort_month,
      {{ cte }}.parent_account_cohort_quarter,
      {{ cte }}.auto_renew_native_hist,
      {{ cte }}.auto_renew_customerdot_hist,
      {{ cte }}.turn_on_cloud_licensing,
      {{ cte }}.turn_on_operational_metrics,
      {{ cte }}.contract_operational_metrics,
      {{ cte }}.contract_auto_renewal,
      {{ cte }}.turn_on_auto_renewal,
      {{ cte }}.contract_seat_reconciliation,
      {{ cte }}.turn_on_seat_reconciliation,
      {{ cte }}.invoice_owner_account,
      {{ cte }}.creator_account,
      {{ cte }}.was_purchased_through_reseller,

      --product info
      {{ cte }}.product_tier_name,
      {{ cte }}.product_delivery_type,
      {{ cte }}.product_deployment_type,
      {{ cte }}.product_category,
      {{ cte }}.product_rate_plan_category,
      {{ cte }}.product_ranking,
      {{ cte }}.service_type,
      {{ cte }}.product_rate_plan_charge_name,
      {{ cte }}.product_rate_plan_name,
      {{ cte }}.is_licensed_user,
      {{ cte }}.is_licensed_user_base_product,
      {{ cte }}.is_arpu,
    
      -- MRR values
      {{ cte }}.unit_of_measure,
      {{ cte }}.mrr,
      {{ cte }}.arr,
      {{ cte }}.quantity

{% endmacro %}