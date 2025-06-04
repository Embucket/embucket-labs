WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'event') }}

), renamed AS (

    SELECT
      id                                     AS event_id,
        
    --keys
      accountid::VARCHAR                     AS account_id,
      ownerid::VARCHAR                       AS owner_id,
      whoid::VARCHAR                         AS lead_or_contact_id,
      whatid::VARCHAR                        AS what_id,
      related_to_id__c::VARCHAR              AS related_to_id,
      createdbyid::VARCHAR                   AS created_by_id,
      booked_by__c::VARCHAR                  AS booked_by_dim_crm_user_id, 
  
    --info      
      subject::VARCHAR                       AS event_subject,
      activity_source__c::VARCHAR            AS event_source,
      outreach_meeting_type__c::VARCHAR      AS outreach_meeting_type,
      type::VARCHAR                          AS event_type,
      eventsubtype::VARCHAR                  AS event_subtype,
      event_disposition__c::VARCHAR          AS event_disposition,
      description::VARCHAR                   AS event_description, 
      booked_by_employee_number__c::VARCHAR  AS booked_by_employee_number,
      sa_activity_type__c::VARCHAR           AS sa_activity_type,
      showas::VARCHAR                        AS event_show_as,
      assigned_to_role__c::VARCHAR           AS assigned_to_role,
      csm_activity_type__c::VARCHAR          AS csm_activity_type,
      customer_interaction_sentiment__c::VARCHAR 
                                             AS customer_interaction_sentiment,
      google_doc_link__c::VARCHAR            AS google_doc_link,
      comments__c::VARCHAR                   AS comments,
      qualified_convo_or_meeting__c::VARCHAR AS qualified_convo_or_meeting,

    --Event Relations Info
      related_to_account__c::VARCHAR         AS related_account_id,
      related_to_account_name__c::VARCHAR    AS related_account_name,
      related_to_lead__c::VARCHAR            AS related_lead_id,
      related_to_opportunity__c::VARCHAR     AS related_opportunity_id,
      related_to_contact__c::VARCHAR         AS related_contact_id, 

    --Dates and Datetimes
      startdatetime::TIMESTAMP               AS event_start_date_time,
      reminderdatetime::TIMESTAMP            AS reminder_date_time,
      enddatetime::TIMESTAMP                 AS event_end_date_time,
      activitydate::DATE                     AS event_date,
      activitydatetime::DATE                 AS event_date_time,
      createddate::TIMESTAMP                 AS created_at,
      enddate::TIMESTAMP                     AS event_end_date,


    --Event Flags
      isalldayevent::BOOLEAN                 AS is_all_day_event, 
      isarchived::BOOLEAN                    AS is_archived,
      ischild::BOOLEAN                       AS is_child_event, 
      isgroupevent::BOOLEAN                  AS is_group_event,
      isprivate::BOOLEAN                     AS is_private_event,
      isrecurrence::BOOLEAN                  AS is_recurrence,
      isreminderset::BOOLEAN                 AS has_reminder_set,
      is_answered__c::FLOAT                  AS is_answered,
      is_correct_contact__c::FLOAT           AS is_correct_contact,
      meeting_cancelled__c::BOOLEAN          AS is_meeting_canceled,
      close_task__c::BOOLEAN                 AS is_closed_event,
      {{ partner_marketing_task_subject_cleaning('subject') }} 
                                             AS partner_marketing_task_subject,  

    --Recurrence Info
      recurrenceactivityid::VARCHAR          AS event_recurrence_activity_id,
      recurrencedayofweekmask::VARCHAR       AS event_recurrence_day_of_week,
      recurrencedayofmonth::VARCHAR          AS event_recurrence_day_of_month, 
      recurrenceenddateonly::TIMESTAMP       AS event_recurrence_end_date,
      recurrenceinstance::VARCHAR            AS event_recurrence_instance,
      recurrenceinterval::VARCHAR            AS event_recurrence_interval,
      recurrencemonthofyear::VARCHAR         AS event_recurrence_month_of_year, 
      recurrencestartdatetime::TIMESTAMP     AS event_recurrence_start_date_time,
      recurrencetimezonesidkey::VARCHAR      AS event_recurrence_timezone_key,
      recurrencetype::VARCHAR                AS event_recurrence_type,
      isrecurrence2exclusion::BOOLEAN        AS is_recurrence_2_exclusion,
      isrecurrence2::BOOLEAN                 AS is_recurrence_2,
      isrecurrence2exception::BOOLEAN        AS is_recurrence_2_exception,

      -- metadata
      lastmodifiedbyid                       AS last_modified_id,
      lastmodifieddate                       AS last_modified_date,
      systemmodstamp,

      isdeleted::BOOLEAN                     AS is_deleted

    FROM source
)

SELECT *
FROM renamed
