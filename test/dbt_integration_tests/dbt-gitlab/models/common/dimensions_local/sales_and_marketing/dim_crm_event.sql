WITH final AS (

    SELECT

    -- Surrogate key
        prep_crm_event.dim_crm_event_sk,

    -- Natural key
        prep_crm_event.event_id,

    -- event infomation
        prep_crm_event.event_subject,
        prep_crm_event.event_source,
        prep_crm_event.outreach_meeting_type,
        prep_crm_event.event_type,
        prep_crm_event.event_disposition,
        prep_crm_event.event_description,
        prep_crm_event.event_subtype,
        prep_crm_event.booked_by_employee_number,
        prep_crm_event.sa_activity_type,
        prep_crm_event.event_show_as,
        prep_crm_event.assigned_to_role,
        prep_crm_event.csm_activity_type,
        prep_crm_event.customer_interaction_sentiment,
        prep_crm_event.google_doc_link,
        prep_crm_event.comments,
        prep_crm_event.qualified_convo_or_meeting,
        prep_crm_event.first_opportunity_event_created_date,
        prep_crm_event.partner_marketing_task_subject,
    
    --Dates and Datetimes
        prep_crm_event.event_start_date_time,
        prep_crm_event.reminder_date_time,
        prep_crm_event.event_end_date_time,
        prep_crm_event.event_date_time,
        prep_crm_event.created_at,
        prep_crm_event.event_end_date,

    --Event Flags
        prep_crm_event.is_all_day_event, 
        prep_crm_event.is_archived,
        prep_crm_event.is_child_event, 
        prep_crm_event.is_group_event,
        prep_crm_event.is_private_event,
        prep_crm_event.is_recurrence,
        prep_crm_event.has_reminder_set,
        prep_crm_event.is_answered,
        prep_crm_event.is_correct_contact,
        prep_crm_event.is_meeting_canceled,
        prep_crm_event.is_closed_event,

    --Recurrence Info
        prep_crm_event.event_recurrence_activity_id,
        prep_crm_event.event_recurrence_day_of_week,
        prep_crm_event.event_recurrence_day_of_month, 
        prep_crm_event.event_recurrence_end_date,
        prep_crm_event.event_recurrence_instance,
        prep_crm_event.event_recurrence_interval,
        prep_crm_event.event_recurrence_month_of_year, 
        prep_crm_event.event_recurrence_start_date_time,
        prep_crm_event.event_recurrence_timezone_key,
        prep_crm_event.event_recurrence_type,
        prep_crm_event.is_recurrence_2_exclusion,
        prep_crm_event.is_recurrence_2,
        prep_crm_event.is_recurrence_2_exception,

      -- metadata
        prep_crm_event.last_modified_id,
        prep_crm_event.last_modified_date,
        prep_crm_event.systemmodstamp

    FROM {{ ref('prep_crm_event') }}
    WHERE is_deleted = FALSE

    )

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2024-07-31"
) }}