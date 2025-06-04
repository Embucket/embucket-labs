{{ simple_cte([
    ('prep_marketo_person','prep_marketo_person')
]) }}

, final AS (

    SELECT
     -- IDs
        prep_marketo_person.dim_marketo_person_id,
        prep_marketo_person.dim_crm_person_id,
        prep_marketo_person.email_hash,

    -- common dimension keys
        prep_marketo_person.acquisition_program_id,
        prep_marketo_person.gitlabdotcom_user_id,
        prep_marketo_person.dim_crm_account_id,

    -- important person dates
        prep_marketo_person.marketo_email_bounced_date,
        prep_marketo_person.marketo_acquisition_date,
        prep_marketo_person.marketo_opt_in_date,
        prep_marketo_person.marketo_created_date_time,
        prep_marketo_person.sign_up_date,
        prep_marketo_person.opt_out_date,
        prep_marketo_person.email_suspended_date,
        prep_marketo_person.initial_start_date,
        prep_marketo_person.customer_health_score_date,
        prep_marketo_person.double_opt_in_date,

    -- person information
        prep_marketo_person.title,
        prep_marketo_person.country,
        prep_marketo_person.sfdc_record_type,
        prep_marketo_person.original_source_type,
        prep_marketo_person.registration_source_type,
        prep_marketo_person.tech_stack,
        prep_marketo_person.original_source_info,
        prep_marketo_person.marketo_person_type,
        prep_marketo_person.email_bounced_reason,
        prep_marketo_person.compliance_segment_value,
        prep_marketo_person.marketo_person_region,
        prep_marketo_person.marketo_account_region,
        prep_marketo_person.preferred_language,
        prep_marketo_person.marketo_account_tier,
        prep_marketo_person.marketo_account_tier_notes,
        prep_marketo_person.marketo_abm_tier,
        prep_marketo_person.account_demographics_upa_country,
        prep_marketo_person.account_demographics_upa_country_name,
        prep_marketo_person.product_category,
        
    -- marketo person dimensions
        prep_marketo_person.marketo_priority,
        prep_marketo_person.marketo_urgency,
        prep_marketo_person.marketo_relative_urgency,
        prep_marketo_person.marketo_lead_source,
        prep_marketo_person.marketo_lead_status,
        prep_marketo_person.marketo_newsletter_segment,
        prep_marketo_person.email_suspended_cause,
        prep_marketo_person.products_purchased,
        prep_marketo_person.marketo_rating_scale,                

    -- person flags
        prep_marketo_person.is_email_bounced,
        prep_marketo_person.is_unsubscribed,
        prep_marketo_person.is_opt_in,
        prep_marketo_person.is_pql_marketo,
        prep_marketo_person.is_paid_tier_marketo,
        prep_marketo_person.is_ptpt_contact_marketo,
        prep_marketo_person.is_ptp_contact_marketo,
        prep_marketo_person.is_impacted_by_user_limit_marketo,
        prep_marketo_person.is_currently_in_trial_marketo,
        prep_marketo_person.is_active_user,
        prep_marketo_person.is_subscribed_webcast,
        prep_marketo_person.is_subscribed_live_events,
        prep_marketo_person.is_gdpr_compliant,
        prep_marketo_person.is_email_suspended,
        prep_marketo_person.has_requested_migration_services,
        prep_marketo_person.has_requested_implementation_services,
        prep_marketo_person.has_requested_specialized_trainings,
        prep_marketo_person.is_core_user,
        prep_marketo_person.is_active_plan,
        prep_marketo_person.is_secure_active,
        prep_marketo_person.is_marketing_suspended,
        prep_marketo_person.is_email_opt_out,
        prep_marketo_person.is_events_segment,
        prep_marketo_person.is_black_listed,
        prep_marketo_person.is_do_not_route,
        prep_marketo_person.is_double_opt_in,
        prep_marketo_person.has_requested_education,
        prep_marketo_person.is_30_day_opt_out,
        prep_marketo_person.is_marketing_comm_opt_out,
        prep_marketo_person.is_using_ce,
        prep_marketo_person.is_do_not_contact,
        prep_marketo_person.is_invalid_email,
        prep_marketo_person.is_high_priority,
        prep_marketo_person.is_unsubscribe_all_marketing,

    -- additive fields
        prep_marketo_person.marketo_relative_person_score,
        prep_marketo_person.marketo_demographic_score,
        prep_marketo_person.marketo_behavior_score,
        prep_marketo_person.license_user_count,
        prep_marketo_person.marketo_health_score,
        prep_marketo_person.engagio_pipeline_predict_score,
        prep_marketo_person.infer_score,
        prep_marketo_person.data_quality_score,
        prep_marketo_person.marketo_lead_score_2

    FROM prep_marketo_person

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-03-28",
    updated_date="2024-04-01"
) }}