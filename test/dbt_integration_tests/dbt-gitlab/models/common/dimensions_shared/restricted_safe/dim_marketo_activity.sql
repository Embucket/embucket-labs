{{ simple_cte([
    ('prep_marketo_activity','prep_marketo_activity')
]) }}
    
, final AS (

    SELECT DISTINCT
        marketo_activity_sk,
        dim_marketo_person_id,
        activity_date,
        activity_datetime,
        campaign_name,
        campaign_type,
        campaign_member_status,
        marketo_form_name,
        form_fields,
        marketo_query_parameters,
        marketo_referrer_url,
        marketo_form_client_ip_address_hash,
        linkedin_form_name,
        webpage,
        webpage_url,
        search_engine,
        search_query,
        marketo_nurture,
        marketo_email_name,
        marketo_email_category,
        marketo_email_bounce_reason,
        marketo_email_subcategory,
        marketo_email_test_variant,
        changed_score_field,
        scoring_rule,
        score_change_original_value,
        score_change_new_value,
        clicked_url,
        push_lead_to_marketo_source,
        marketo_score_decay_reason,
        marketo_score_decay_type,
        trial_type,
        activity_type,
        in_current_scoring_model,
        is_person_activity,
        scored_action
    FROM prep_marketo_activity

)

SELECT *
FROM final