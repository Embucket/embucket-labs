WITH final AS (


  SELECT

    -- Surrogate key
    prep_crm_task.dim_crm_task_sk, 

    -- Natural key
    prep_crm_task.task_id,

    -- Task infomation
    prep_crm_task.full_comments,
    prep_crm_task.task_subject,
    prep_crm_task.partner_marketing_task_subject,
    prep_crm_task.task_status,
    prep_crm_task.task_subtype,
    prep_crm_task.task_type,
    prep_crm_task.task_priority,
    prep_crm_task.close_task,
    prep_crm_task.is_closed,
    prep_crm_task.is_deleted,
    prep_crm_task.is_archived,
    prep_crm_task.is_high_priority,
    prep_crm_task.persona_functions,
    prep_crm_task.persona_levels,
    prep_crm_task.outreach_meeting_type,
    prep_crm_task.customer_interaction_sentiment,
    prep_crm_task.task_owner_role,

    -- Activity infromation
    prep_crm_task.activity_disposition,
    prep_crm_task.activity_source,
    prep_crm_task.csm_activity_type,
    prep_crm_task.sa_activity_type,
    prep_crm_task.gs_activity_type,
    prep_crm_task.gs_sentiment,
    prep_crm_task.gs_meeting_type,
    prep_crm_task.is_gs_exec_sponsor_present,
    prep_crm_task.is_meeting_cancelled,
    prep_crm_task.products_positioned,

    -- Call information
    prep_crm_task.call_type,
    prep_crm_task.call_purpose,
    prep_crm_task.call_disposition,
    prep_crm_task.call_duration_in_seconds,
    prep_crm_task.call_recording,
    prep_crm_task.is_answered,
    prep_crm_task.is_correct_contact,

    -- Reminder information
    prep_crm_task.is_reminder_set,

    -- Recurrence information
    prep_crm_task.is_recurrence,
    prep_crm_task.task_recurrence_interval,
    prep_crm_task.task_recurrence_instance,
    prep_crm_task.task_recurrence_type,
    prep_crm_task.task_recurrence_activity_id,
    prep_crm_task.task_recurrence_day_of_week,
    prep_crm_task.task_recurrence_timezone,
    prep_crm_task.task_recurrence_day_of_month,
    prep_crm_task.task_recurrence_month,

    -- Sequence information
    prep_crm_task.active_sequence_name,
    prep_crm_task.sequence_step_number,

    -- Docs/Video Conferencing
    prep_crm_task.google_doc_link,
    prep_crm_task.zoom_app_ics_sequence,
    prep_crm_task.zoom_app_use_personal_zoom_meeting_id,
    prep_crm_task.zoom_app_join_before_host,
    prep_crm_task.zoom_app_make_it_zoom_meeting,
    prep_crm_task.chorus_call_id,

    -- Flags
    prep_crm_task.is_reminder_task,
    prep_crm_task.is_completed_task,
    prep_crm_task.is_gainsight_integration_user_task,
    prep_crm_task.is_demand_gen_task,
    prep_crm_task.is_demo_task,
    prep_crm_task.is_workshop_task,
    prep_crm_task.is_meeting_task,
    prep_crm_task.is_email_task,
    prep_crm_task.is_incoming_email_task,
    prep_crm_task.is_outgoing_email_task,
    prep_crm_task.is_high_priority_email_task,
    prep_crm_task.is_low_priority_email_task,
    prep_crm_task.is_normal_priority_email_task,
    prep_crm_task.is_call_task,
    prep_crm_task.is_call_longer_1min_task,
    prep_crm_task.is_high_priority_call_task,
    prep_crm_task.is_low_priority_call_task,
    prep_crm_task.is_normal_priority_call_task,
    prep_crm_task.is_not_answered_call_task,
    prep_crm_task.is_answered_meaningless_call_task,
    prep_crm_task.is_answered_meaningfull_call_task,
    prep_crm_task.is_opportunity_initiation_email_task,
    prep_crm_task.is_opportunity_followup_email_task,
    prep_crm_task.is_opportunity_initiation_call_task,
    prep_crm_task.is_opportunity_followup_call_task,
    prep_crm_task.is_created_by_groove

    FROM {{ ref('prep_crm_task') }}
    WHERE is_deleted = FALSE


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-12-05",
    updated_date="2025-01-15"
) }}
