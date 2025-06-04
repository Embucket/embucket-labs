WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_task_source') }}

), renamed AS(

    SELECT
      task_id, 

      --keys
      {{ dbt_utils.generate_surrogate_key(['source.task_id']) }}             AS dim_crm_task_sk,
      source.task_id                                                AS dim_crm_task_pk,
      source.account_id                                             AS dim_crm_account_id,
      source.owner_id                                               AS dim_crm_user_id,
      source.assigned_employee_number,
      source.lead_or_contact_id,
      source.account_or_opportunity_id,
      source.record_type_id                                         AS sfdc_record_type_id,
      source.related_to_account_name,
      source.related_lead_id,
      source.related_contact_id,
      source.related_opportunity_id                                 AS dim_crm_opportunity_id,
      source.related_account_id,
      source.related_to_id,
      COALESCE(source.related_lead_id, source.related_contact_id)   AS sfdc_record_id,

      -- Task infomation
      source.comments,
      source.full_comments,
      source.task_subject,
      source.partner_marketing_task_subject,
      source.task_date,
      source.task_created_at                                        AS task_created_date,
      FIRST_VALUE(source.task_created_at) OVER (PARTITION BY source.related_opportunity_id ORDER BY source.task_created_at ASC) AS first_opportunity_task_created_date,
      source.task_created_by_id,
      source.task_status,
      source.task_subtype,
      source.task_type,
      source.task_priority,
      source.close_task,
      source.task_completed_at                                      AS task_completed_date,
      source.is_closed,
      source.is_deleted,
      source.is_archived,
      source.is_high_priority,
      source.persona_functions,
      source.persona_levels,
      source.outreach_meeting_type,
      source.customer_interaction_sentiment,
      source.task_owner_role,
      source.is_created_by_groove,

      -- Activity infromation
      source.activity_disposition,
      source.activity_source,
      source.csm_activity_type,
      source.sa_activity_type,
      source.gs_activity_type,
      source.gs_sentiment,
      source.gs_meeting_type,
      source.is_gs_exec_sponsor_present,
      source.is_meeting_cancelled,
      source.products_positioned,

      -- Call information
      source.call_type,
      source.call_purpose,
      source.call_disposition,
      source.call_duration_in_seconds,
      source.call_recording,
      source.is_answered,
      source.is_correct_contact,

      -- Reminder information
      source.is_reminder_set,
      source.reminder_at                                            AS reminder_date,

      -- Recurrence information
      source.is_recurrence,
      source.task_recurrence_interval,
      source.task_recurrence_instance,
      source.task_recurrence_type,
      source.task_recurrence_activity_id,
      source.task_recurrence_end_date                               AS task_recurrence_date,
      source.task_recurrence_day_of_week,
      source.task_recurrence_timezone,
      source.task_recurrence_start_date,
      source.task_recurrence_day_of_month,
      source.task_recurrence_month,

      -- Sequence information
      source.active_sequence_name,
      source.sequence_step_number,

      -- Docs/Video Conferencing
      source.google_doc_link,
      source.zoom_app_ics_sequence,
      source.zoom_app_use_personal_zoom_meeting_id,
      source.zoom_app_join_before_host,
      source.zoom_app_make_it_zoom_meeting,
      source.chorus_call_id,

      -- Counts
      source.account_or_opportunity_count,
      source.lead_or_contact_count,

      -- metadata
      source.last_modified_id,
      source.last_modified_at                                                                           AS last_modified_date,
      source.system_modified_at                                                                         AS systemmodstamp,

      -- flags
      IFF(
        source.task_subject LIKE '%Reminder%',
          1,
        0
        )                                                                                               AS is_reminder_task,
      IFF(
        source.task_status = 'Completed', 
          1,
        0
        )                                                                                               AS is_completed_task,
      IFF(
        source.owner_id != '0054M000004M9pdQAC', 
          1,
        0
        )                                                                                               AS is_gainsight_integration_user_task,
      IFF(
        source.task_type ='Demand Gen' 
          OR LOWER(source.task_subject) LIKE '%demand gen%'
            OR LOWER(source.full_comments) LIKE '%demand gen%',
          1,
        0
        )                                                                                               AS is_demand_gen_task,
       IFF(
        source.task_type ='Demo' 
          OR LOWER(source.task_subject) LIKE '%demo%'
            OR LOWER(source.full_comments) LIKE '%demo%',
           1,
        0
        )                                                                                                AS is_demo_task,
       IFF(
        source.task_type ='Workshop' 
          OR LOWER(source.task_subject) LIKE '%workshop%'
            OR LOWER(source.full_comments) LIKE '%workshop%',
          1,
        0
        )                                                                                                AS is_workshop_task,
      IFF(
        source.task_type LIKE '%meeting%' 
          OR LOWER(source.task_subject) LIKE '%meeting%'
            OR LOWER(source.full_comments) LIKE '%meeting%',
          1,
        0
        )                                                                                               AS is_meeting_task,
      IFF(
        (
        source.task_type='Email' 
          AND LOWER(source.task_subject) LIKE '%[email] [out]%'
          )
            OR (
                source.task_type ='Other' 
                  AND LOWER(source.task_subject) LIKE '%email sent%'
                  ),
          1,
        0
        )                                                                                               AS is_email_task,
      IFF(
        source.task_subject LIKE '%[In]%', 
          1,
         0
         )                                                                                              AS is_incoming_email_task,
      IFF(
        source.task_subject LIKE '%[Out]%', 
          1,
         0
         )                                                                                              AS is_outgoing_email_task,
      IFF(
        is_email_task = 1
          AND LOWER(source.task_priority) ='high',
          1,
        0
        )                                                                                               AS is_high_priority_email_task,
      IFF(
        is_email_task = 1
          AND LOWER(source.task_priority) ='low',
            1,
          0
          )                                                                                             AS is_low_priority_email_task,
       IFF(
        is_email_task = 1
          AND LOWER(source.task_priority) ='normal',
            1,
          0
          )                                                                                             AS is_normal_priority_email_task,
        IFF(
          source.task_type='Call' 
            OR source.task_subtype ='Call', 
            1,
          0
          )                                                                                             AS is_call_task,
        IFF(
          is_call_task = 1
            AND source.call_duration_in_seconds >= 60,
            1,
          0
          )                                                                                             AS is_call_longer_1min_task,
        IFF(
          is_call_task = 1
            AND lower(source.task_priority) ='high',
            1,
          0
          )                                                                                             AS is_high_priority_call_task,
        IFF(
          is_call_task = 1
            AND lower(source.task_priority) ='low',
            1,
          0
          )                                                                                             AS is_low_priority_call_task,
        IFF(
          is_call_task = 1
            AND lower(source.task_priority) ='normal',
            1,
          0
          )                                                                                             AS is_normal_priority_call_task,
        IFF(
          is_call_task = 1
            AND (
                 source.call_disposition IN (
                                            'Not Answered','Correct Contact: Not Answered/Other','Call - No Answer','No Number Found','Busy',
                                            'Bad Number','Automated Switchboard','Incorrect Contact: Left Message','Not Answered (legacy)',
                                            'Incorrect Contact: Not Answered/Other','Main Company Line - Can''t Transfer Line','',' '
                                            )
                  OR source.call_disposition IS NULL
                  ),
            1,
          0
          )                                                                                             AS is_not_answered_call_task,
        IFF(
          is_call_task = 1
            AND source.call_disposition IN (
                                            'CC: Answered: Info Gathered: Not Opp yet',
                                            'CC: Answered: Not Interested','Incorrect Contact: Answered','CC: Answered: Personal Use'
                                            ),
            1,
          0
          )                                                                                             AS is_answered_meaningless_call_task,
        IFF(
          is_call_task = 1
            AND source.call_disposition IN (
                                            'CC:Answered: Info Gathered: Potential Opp',
                                            'Correct Contact: Answered','CC: Answered: Asked for Call Back','Correct Contact: IQM Set',
                                            'Correct Contact: Discovery Call Set','CC: Answered: Using Competition','Incorrect Contact: Answered: Gave Referral',
                                            'Correct Contact: Answered (Do not use)','Answered (legacy)'
                                            ),
            1,
          0
          )                                                                                             AS is_answered_meaningfull_call_task,
        IFF(
          is_email_task = 1
            AND source.task_created_at = first_opportunity_task_created_date,
            1,
          0
          )                                                                                             AS is_opportunity_initiation_email_task,
        IFF(
          is_email_task = 1
            AND source.task_created_at != first_opportunity_task_created_date,
              1,
            0
            )                                                                                           AS is_opportunity_followup_email_task,
        IFF(
          is_call_task = 1
            AND  source.task_created_at = first_opportunity_task_created_date,
            1,
          0
          )                                                                                             AS is_opportunity_initiation_call_task,
        IFF(
          is_call_task = 1
            AND  source.task_created_at != first_opportunity_task_created_date,
            1,
          0
          )                                                                                             AS is_opportunity_followup_call_task,

      -- Calculated averaged and percents
       DATEDIFF(hour, source.task_date, source.task_created_at)                                       AS hours_waiting_before_task,
       ROUND(
        IFF(
            is_email_task = 1,
              hours_waiting_before_task,
            NULL
            ),
          1)                                                                                            AS hours_waiting_before_email_task,
       ROUND(
         IFF(
           is_call_task = 1,
             source.call_duration_in_seconds,
           NULL
           ),
        0
        )                                                                                              AS call_task_duration_in_seconds,
       ROUND(
         IFF(
           is_call_task = 1,
             hours_waiting_before_task,
           NULL
           ),
        1)                                                                                              AS hours_waiting_before_call_task



    FROM source
)

SELECT *
FROM renamed
