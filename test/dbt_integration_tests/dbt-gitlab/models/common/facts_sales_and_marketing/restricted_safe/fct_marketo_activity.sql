{{ simple_cte([
    ('prep_marketo_activity','prep_marketo_activity'),
    ('sheetload_marketo_lead_scores_source','sheetload_marketo_lead_scores_source')
]) }}

, final AS (

    SELECT
        prep_marketo_activity.marketo_activity_sk,
        prep_marketo_activity.dim_marketo_person_id,
        prep_marketo_activity.dim_crm_person_id,
        prep_marketo_activity.sfdc_record_id,
        prep_marketo_activity.dim_marketo_activity_id,
        prep_marketo_activity.campaign_id,
        prep_marketo_activity.marketo_campaign_id,
        prep_marketo_activity.marketo_form_id,
        prep_marketo_activity.marketo_webpage_id,
        prep_marketo_activity.marketo_nurture_id,
        prep_marketo_activity.marketo_nurture_previous_track_id,
        prep_marketo_activity.marketo_nurture_new_track_id,
        prep_marketo_activity.web_activity_type_id,
        prep_marketo_activity.marketo_email_id,
        prep_marketo_activity.marketo_campaign_run_id,
        prep_marketo_activity.marketo_email_program_step_id,
        prep_marketo_activity.push_lead_to_marketo_source_id,

    -- Lead Scores
        prep_marketo_activity.score_change_net_value,
        sheetload_marketo_lead_scores_source.current_score,
        sheetload_marketo_lead_scores_source.previous_score,
        sheetload_marketo_lead_scores_source.test_score
    FROM prep_marketo_activity
    LEFT JOIN sheetload_marketo_lead_scores_source
        ON prep_marketo_activity.activity_type=sheetload_marketo_lead_scores_source.activity_type
            AND (prep_marketo_activity.scored_action=sheetload_marketo_lead_scores_source.scored_action
                OR sheetload_marketo_lead_scores_source.scored_action IS NULL)

)

SELECT *
FROM final