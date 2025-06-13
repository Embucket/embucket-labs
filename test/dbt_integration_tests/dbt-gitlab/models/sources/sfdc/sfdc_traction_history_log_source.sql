WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'traction_history_log') }}

), renamed AS (

    SELECT
        id                                            AS traction_history_log_id,

    -- keys
        createdbyid                                   AS traction_history_log_created_by_id,
        lastmodifiedbyid                              AS traction_history_log_last_modified_by_id,
        ownerid                                       AS traction_history_log_owner_id,
        tracrtc__object_id__c                         AS traction_history_log_object_id,
        tracrtc__flow_id__c                           AS traction_history_log_flow_id,
        tracrtc__execution_id__c                      AS traction_history_log_execution_id,
        tracrtc__lead__c                              AS traction_history_log_lead_id,
        tracrtc__contact__c                           AS traction_history_log_contact_id,
        tracrtc__case__c                              AS traction_history_log_case_id,
        tracrtc__account__c                           AS traction_history_log_account_id,
        tracrtc__opportunity__c                       AS traction_history_log_opportunity_id,

    -- date/time
        createddate                                   AS traction_history_log_created_datetime,
        lastmodifieddate                              AS traction_history_log_last_modified_datetime,
        systemmodstamp                                AS traction_history_log_system_modified_datetime,
        tracrtc__log_date__c                          AS traction_history_log_log_datetime,
        tracrtc__log_sort__c                          AS traction_history_log_sort_datetime,
        tracrtc__history_index__c                     AS traction_history_log_index_datetime,

    -- log details
        name                                          AS traction_history_log_name,
        tracrtc__record_type_name__c                  AS traction_history_log_record_type_name,
        tracrtc__log_number__c                        AS traction_history_log_number,
        tracrtc__case_evaluated__c                    AS traction_history_log_evaluated_cases,
        tracrtc__flow_name__c                         AS traction_history_log_flow_name,
        tracrtc__flow_step__c                         AS traction_history_log_flow_step,
        tracrtc__flow_step_evaluated_as__c            AS is_traction_history_log_flow_step_evaluated,
        tracrtc__flow_step_snapshot__c                AS traction_history_log_flow_step_snapshot_array,
        tracrtc__realtime_clean_error_message__c      AS traction_history_log_error_message,
        tracrtc__realtime_clean_processed__c          AS is_processed_traction_history_log,

    -- modifications
        tracrtc__modified_fields__c                   AS traction_history_log_modified_fields_array,
        tracrtc__evaluated_fields__c                  AS traction_history_log_evaluated_fields_array,
        tracrtc__previous_field_value__c              AS traction_history_log_previous_field_value,

    -- other
        isdeleted                                     AS is_deleted
    FROM base

)

SELECT *
FROM renamed
        
        
        
        
        
        
        
        
        
        