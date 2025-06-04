{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_crm_task', 'dim_crm_task'),
    ('fct_crm_task', 'fct_crm_task')
]) }}

, final AS (


  SELECT
    -- Primary Key
    fct_crm_task.dim_crm_task_pk,

    -- Surrogate key
    dim_crm_task.dim_crm_task_sk,

    -- Natural key
    dim_crm_task.task_id,

    -- Foreign keys
    fct_crm_task.dim_crm_account_id,
    fct_crm_task.dim_crm_user_id,
    fct_crm_task.sfdc_record_type_id,
    fct_crm_task.dim_crm_person_id,
    fct_crm_task.sfdc_record_id,
    fct_crm_task.dim_crm_opportunity_id,
    fct_crm_task.dim_mapped_opportunity_id,

    -- Task infomation
    fct_crm_task.task_date_id,
    fct_crm_task.task_date,
    fct_crm_task.task_completed_date_id,
    fct_crm_task.task_completed_date,
    fct_crm_task.task_mapped_to,
    dim_crm_task.full_comments,
    dim_crm_task.task_subject,
    dim_crm_task.partner_marketing_task_subject,
    dim_crm_task.task_status,
    dim_crm_task.task_subtype,
    dim_crm_task.task_type,
    dim_crm_task.task_priority,
    dim_crm_task.close_task,
    dim_crm_task.is_closed,
    dim_crm_task.is_deleted,
    dim_crm_task.is_archived,
    dim_crm_task.is_high_priority,
    dim_crm_task.persona_functions,
    dim_crm_task.persona_levels,
    dim_crm_task.outreach_meeting_type,
    dim_crm_task.customer_interaction_sentiment,
    dim_crm_task.task_owner_role,

    -- Activity infromation
    dim_crm_task.activity_disposition,
    dim_crm_task.activity_source,
    dim_crm_task.csm_activity_type,
    dim_crm_task.sa_activity_type,
    dim_crm_task.gs_activity_type,
    dim_crm_task.gs_sentiment,
    dim_crm_task.gs_meeting_type,
    dim_crm_task.is_gs_exec_sponsor_present,
    dim_crm_task.is_meeting_cancelled,
    dim_crm_task.products_positioned,

    -- Call information
    dim_crm_task.call_type,
    dim_crm_task.call_purpose,
    dim_crm_task.call_disposition,
    dim_crm_task.call_duration_in_seconds,
    dim_crm_task.call_recording,
    dim_crm_task.is_answered,
    dim_crm_task.is_correct_contact,

    -- Reminder information
    dim_crm_task.is_reminder_set,
    fct_crm_task.reminder_date_id,
    fct_crm_task.reminder_date,

    -- Recurrence information
    dim_crm_task.is_recurrence,
    fct_crm_task.task_recurrence_date_id,
    fct_crm_task.task_recurrence_date,
    fct_crm_task.task_recurrence_start_date_id,
    fct_crm_task.task_recurrence_start_date,
    dim_crm_task.task_recurrence_interval,
    dim_crm_task.task_recurrence_instance,
    dim_crm_task.task_recurrence_type,
    dim_crm_task.task_recurrence_activity_id,
    dim_crm_task.task_recurrence_day_of_week,
    dim_crm_task.task_recurrence_timezone,
    dim_crm_task.task_recurrence_day_of_month,
    dim_crm_task.task_recurrence_month,

    -- Sequence information
    dim_crm_task.active_sequence_name,
    dim_crm_task.sequence_step_number,

    -- Docs/Video Conferencing
    dim_crm_task.google_doc_link,
    dim_crm_task.zoom_app_ics_sequence,
    dim_crm_task.zoom_app_use_personal_zoom_meeting_id,
    dim_crm_task.zoom_app_join_before_host,
    dim_crm_task.zoom_app_make_it_zoom_meeting,
    dim_crm_task.chorus_call_id,

    --sales dev hierarchy fields
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_manager_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_leader_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_1,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_2,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_3,

    -- Counts
    fct_crm_task.account_or_opportunity_count,
    fct_crm_task.lead_or_contact_count,

    -- Flags
    dim_crm_task.is_reminder_task,
    dim_crm_task.is_completed_task,
    dim_crm_task.is_gainsight_integration_user_task,
    dim_crm_task.is_demand_gen_task,
    dim_crm_task.is_demo_task,
    dim_crm_task.is_workshop_task,
    dim_crm_task.is_meeting_task,
    dim_crm_task.is_email_task,
    dim_crm_task.is_incoming_email_task,
    dim_crm_task.is_outgoing_email_task,
    dim_crm_task.is_high_priority_email_task,
    dim_crm_task.is_low_priority_email_task,
    dim_crm_task.is_normal_priority_email_task,
    dim_crm_task.is_call_task,
    dim_crm_task.is_call_longer_1min_task,
    dim_crm_task.is_high_priority_call_task,
    dim_crm_task.is_low_priority_call_task,
    dim_crm_task.is_normal_priority_call_task,
    dim_crm_task.is_not_answered_call_task,
    dim_crm_task.is_answered_meaningless_call_task,
    dim_crm_task.is_answered_meaningfull_call_task,
    dim_crm_task.is_opportunity_initiation_email_task,
    dim_crm_task.is_opportunity_followup_email_task,
    dim_crm_task.is_opportunity_initiation_call_task,
    dim_crm_task.is_opportunity_followup_call_task,
    dim_crm_task.is_created_by_groove,
    fct_crm_task.hours_waiting_before_task,
    fct_crm_task.hours_waiting_before_email_task,
    fct_crm_task.call_task_duration_in_seconds,
    fct_crm_task.hours_waiting_before_call_task,

    -- Metadata
    fct_crm_task.task_created_by_id,
    fct_crm_task.task_created_date_id,
    fct_crm_task.task_created_date,
    fct_crm_task.last_modified_id,
    fct_crm_task.last_modified_date_id,
    fct_crm_task.last_modified_date

    FROM fct_crm_task 
    LEFT JOIN dim_crm_task 
      ON fct_crm_task.dim_crm_task_sk = dim_crm_task.dim_crm_task_sk
    LEFT JOIN {{ref('dim_sales_dev_user_hierarchy')}}
      ON fct_crm_task.dim_crm_user_id=dim_sales_dev_user_hierarchy.dim_crm_user_id
        AND fct_crm_task.task_completed_date=dim_sales_dev_user_hierarchy.snapshot_date


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-12-05",
    updated_date="2025-01-15"
) }}
