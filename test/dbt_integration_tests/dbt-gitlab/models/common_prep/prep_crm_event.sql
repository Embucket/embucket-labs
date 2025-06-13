WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_event_source') }}

), renamed AS(

    SELECT
      event_id, 

    --keys
      {{ dbt_utils.generate_surrogate_key(['source.event_id']) }}            AS dim_crm_event_sk,
      source.event_id                                               AS dim_crm_event_pk,
      source.account_id                                             AS dim_crm_account_id,
      source.owner_id                                               AS dim_crm_user_id,
      source.lead_or_contact_id,
      source.related_account_name,
      source.related_lead_id,
      source.related_contact_id,
      source.related_opportunity_id                                 AS dim_crm_opportunity_id,
      source.related_account_id,
      source.related_to_id,
      source.created_by_id,
      source.booked_by_dim_crm_user_id,
      source.what_id,
      COALESCE(source.related_lead_id, source.related_contact_id)   AS sfdc_record_id,

    -- Task infomation
      source.event_subject,
      source.event_source,
      source.outreach_meeting_type,
      source.event_type,
      source.event_disposition,
      source.event_description, 
      source.event_subtype,
      source.booked_by_employee_number,
      source.sa_activity_type,
      source.event_show_as,
      source.assigned_to_role,
      source.csm_activity_type,
      source.customer_interaction_sentiment,
      source.google_doc_link,
      source.comments,
      source.qualified_convo_or_meeting,
      FIRST_VALUE(source.created_at) OVER (PARTITION BY source.related_opportunity_id ORDER BY source.created_at ASC) AS first_opportunity_event_created_date,
      source.partner_marketing_task_subject,
    
    --Dates and Datetimes
      source.event_start_date_time,
      source.reminder_date_time,
      source.event_end_date_time,
      source.event_date,
      source.event_date_time,
      source.created_at,
      source.event_end_date,


    --Event Flags
      source.is_all_day_event, 
      source.is_archived,
      source.is_child_event, 
      source.is_group_event,
      source.is_private_event,
      source.is_recurrence,
      source.has_reminder_set,
      source.is_answered,
      source.is_correct_contact,
      source.is_meeting_canceled,
      source.is_closed_event,

    --Recurrence Info
      source.event_recurrence_activity_id,
      source.event_recurrence_day_of_week,
      source.event_recurrence_day_of_month, 
      source.event_recurrence_end_date,
      source.event_recurrence_instance,
      source.event_recurrence_interval,
      source.event_recurrence_month_of_year, 
      source.event_recurrence_start_date_time,
      source.event_recurrence_timezone_key,
      source.event_recurrence_type,
      source.is_recurrence_2_exclusion,
      source.is_recurrence_2,
      source.is_recurrence_2_exception,

      -- metadata
      source.last_modified_id,
      source.last_modified_date,
      source.systemmodstamp,
      source.is_deleted

    FROM source
)

SELECT *
FROM renamed