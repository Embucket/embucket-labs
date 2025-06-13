{{ simple_cte([
    ('dim_marketo_activity','dim_marketo_activity'),
    ('fct_marketo_activity','fct_marketo_activity')
]) }}
    
, final AS (

    SELECT DISTINCT
  -- Primary Key
        dim_marketo_activity.marketo_activity_sk,

  -- IDs
        fct_marketo_activity.dim_marketo_person_id,
        fct_marketo_activity.dim_crm_person_id,
        fct_marketo_activity.sfdc_record_id,
        fct_marketo_activity.dim_marketo_activity_id,
        fct_marketo_activity.campaign_id,
        fct_marketo_activity.marketo_campaign_id,
        fct_marketo_activity.marketo_form_id,
        fct_marketo_activity.marketo_webpage_id,
        fct_marketo_activity.marketo_nurture_id,
        fct_marketo_activity.marketo_nurture_previous_track_id,
        fct_marketo_activity.marketo_nurture_new_track_id,
        fct_marketo_activity.web_activity_type_id,
        fct_marketo_activity.marketo_email_id,
        fct_marketo_activity.marketo_campaign_run_id,
        fct_marketo_activity.marketo_email_program_step_id,
        fct_marketo_activity.push_lead_to_marketo_source_id,

  -- Dates & Date/Times
        dim_marketo_activity.activity_date,
        dim_marketo_activity.activity_datetime,

  -- Other Dimensions
        dim_marketo_activity.campaign_name,
        dim_marketo_activity.campaign_type,
        dim_marketo_activity.campaign_member_status,
        dim_marketo_activity.marketo_form_name,
        dim_marketo_activity.form_fields,
        dim_marketo_activity.marketo_query_parameters,
        dim_marketo_activity.marketo_referrer_url,
        dim_marketo_activity.marketo_form_client_ip_address_hash,
        dim_marketo_activity.linkedin_form_name,
        dim_marketo_activity.webpage,
        dim_marketo_activity.webpage_url,
        dim_marketo_activity.search_engine,
        dim_marketo_activity.search_query,
        dim_marketo_activity.marketo_nurture,
        dim_marketo_activity.marketo_email_name,
        dim_marketo_activity.marketo_email_category,
        dim_marketo_activity.marketo_email_bounce_reason,
        dim_marketo_activity.marketo_email_subcategory,
        dim_marketo_activity.marketo_email_test_variant,
        dim_marketo_activity.changed_score_field,
        dim_marketo_activity.scoring_rule,
        dim_marketo_activity.score_change_original_value,
        dim_marketo_activity.score_change_new_value,
        dim_marketo_activity.clicked_url,
        dim_marketo_activity.push_lead_to_marketo_source,
        dim_marketo_activity.marketo_score_decay_reason,
        dim_marketo_activity.marketo_score_decay_type,
        dim_marketo_activity.trial_type,
        dim_marketo_activity.activity_type,
        dim_marketo_activity.in_current_scoring_model,
        dim_marketo_activity.scored_action,
        dim_marketo_activity.is_person_activity,

  -- Lead Scores
        fct_marketo_activity.score_change_net_value,
        fct_marketo_activity.current_score,
        fct_marketo_activity.previous_score,
        fct_marketo_activity.test_score
    FROM dim_marketo_activity
    LEFT JOIN fct_marketo_activity
        ON dim_marketo_activity.marketo_activity_sk = fct_marketo_activity.marketo_activity_sk

)

SELECT *
FROM final