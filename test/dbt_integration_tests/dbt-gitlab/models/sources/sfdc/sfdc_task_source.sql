WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'task') }}

), renamed AS(

    SELECT
      id                                        AS task_id, 

      --keys
      accountid                                 AS account_id,
      ownerid                                   AS owner_id,
      assigned_employee_number__c               AS assigned_employee_number,
      whoid                                     AS lead_or_contact_id,
      whatid                                    AS account_or_opportunity_id,
      recordtypeid                              AS record_type_id,
      related_to_account_name__c                AS related_to_account_name,
      related_to_lead__c                        AS related_lead_id,
      related_to_contact__c                     AS related_contact_id,
      related_to_opportunity__c                 AS related_opportunity_id,
      related_to_account__c                     AS related_account_id,
      related_to_id__c                          AS related_to_id,

      -- Task infomation
      comments__c                               AS comments,
      description                               AS full_comments,
      subject                                   AS task_subject,
      {{ partner_marketing_task_subject_cleaning('subject') }} 
                                                AS partner_marketing_task_subject,
      activitydate::DATE                        AS task_date,
      createddate::TIMESTAMP                    AS task_created_at,
      createdbyid                               AS task_created_by_id,
      status                                    AS task_status,
      tasksubtype                               AS task_subtype,
      type                                      AS task_type,
      priority                                  AS task_priority,
      close_task__c                             AS close_task,
      completeddatetime::TIMESTAMP              AS task_completed_at,
      isclosed                                  AS is_closed,
      isdeleted                                 AS is_deleted,
      isarchived                                AS is_archived,
      ishighpriority                            AS is_high_priority,
      persona_functions__c                      AS persona_functions,
      persona_levels__c                         AS persona_levels,
      outreach_meeting_type__c                  AS outreach_meeting_type,
      customer_interaction_sentiment__c         AS customer_interaction_sentiment,
      assigned_to_role__c                       AS task_owner_role,
      dascoopcomposer__is_created_by_groove__c  AS is_created_by_groove,

      -- Activity infromation
      activity_disposition__c                   AS activity_disposition,
      activity_source__c                        AS activity_source,
      csm_activity_type__c                      AS csm_activity_type,
      sa_activity_type__c                       AS sa_activity_type,
      gs_activity_type__c                       AS gs_activity_type,
      gs_sentiment__c                           AS gs_sentiment,
      gs_meeting_type__c                        AS gs_meeting_type,
      gs_exec_sponsor_present__c                AS is_gs_exec_sponsor_present,
      meeting_cancelled__c                      AS is_meeting_cancelled,
      products_positioned__c                    AS products_positioned,

      -- Call information
      calltype                                  AS call_type,
      call_purpose__c                           AS call_purpose,
      calldisposition                           AS call_disposition,
      calldurationinseconds                     AS call_duration_in_seconds,
      call_recording__c                         AS call_recording,
      is_answered__c                            AS is_answered,
      is_correct_contact__c                     AS is_correct_contact,

      -- Reminder information
      isreminderset                             AS is_reminder_set,
      reminderdatetime::TIMESTAMP               AS reminder_at,

      -- Recurrence information
      isrecurrence                              AS is_recurrence,
      recurrenceinterval                        AS task_recurrence_interval,
      recurrenceinstance                        AS task_recurrence_instance,
      recurrencetype                            AS task_recurrence_type,
      recurrenceactivityid                      AS task_recurrence_activity_id,
      recurrenceenddateonly::DATE               AS task_recurrence_end_date,
      recurrencedayofweekmask                   AS task_recurrence_day_of_week,
      recurrencetimezonesidkey                  AS task_recurrence_timezone,
      recurrencestartdateonly::DATE             AS task_recurrence_start_date,
      recurrencedayofmonth                      AS task_recurrence_day_of_month,
      recurrencemonthofyear                     AS task_recurrence_month,

      -- Sequence information
      name_of_active_sequence__c                AS active_sequence_name,
      sequence_step_number__c                   AS sequence_step_number,

      -- Docs/Video Conferencing
      google_doc_link__c                        AS google_doc_link,
      zoom_app__ics_sequence__c                 AS zoom_app_ics_sequence,
      zoom_app__use_personal_zoom_meeting_id__c AS zoom_app_use_personal_zoom_meeting_id,
      zoom_app__join_before_host__c             AS zoom_app_join_before_host,
      zoom_app__make_it_zoom_meeting__c         AS zoom_app_make_it_zoom_meeting,
      affectlayer__chorus_call_id__c            AS chorus_call_id,

      -- Counts
      whatcount                                 AS account_or_opportunity_count,
      whocount                                  AS lead_or_contact_count,

      -- metadata
      lastmodifiedbyid                          AS last_modified_id,
      lastmodifieddate::TIMESTAMP               AS last_modified_at,
      systemmodstamp::TIMESTAMP                 AS system_modified_at

    FROM source
)

SELECT *
FROM renamed
