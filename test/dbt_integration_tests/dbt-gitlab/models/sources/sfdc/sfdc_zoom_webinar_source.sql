{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

  SELECT *
  FROM {{ source('salesforce', 'zoom_webinar') }}

),

renamed AS (

  SELECT
    createdbyid::TEXT                                         AS created_by_id,
    createddate::TIMESTAMP_TZ                                 AS created_date,
    id::TEXT                                                  AS id,
    isdeleted::BOOLEAN                                        AS is_deleted,
    lastmodifiedbyid::TEXT                                    AS last_modified_by_id,
    lastmodifieddate::TIMESTAMP_TZ                            AS last_modified_date,
    name::TEXT                                                AS name,
    ownerid::TEXT                                             AS owner_id,
    systemmodstamp::TIMESTAMP_TZ                              AS system_mod_stamp,
    zoom_app__actual_end_time__c::TIMESTAMP_TZ                AS zoom_app_actual_end_time,
    zoom_app__actual_start_time__c::TIMESTAMP_TZ              AS zoom_app_actual_start_time,
    zoom_app__add_registrants_as_campaign_members__c::BOOLEAN AS zoom_app_add_registrants_as_campaign_members,
    zoom_app__agenda__c::TEXT                                 AS zoom_app_agenda,
    zoom_app__create_campaign_for_each_webinar__c::BOOLEAN    AS zoom_app_create_campaign_for_each_webinar,
    zoom_app__create_new_lead__c::BOOLEAN                     AS zoom_app_create_new_lead,
    zoom_app__creation_status__c::TEXT                        AS zoom_app_creation_status,
    zoom_app__duration__c::FLOAT                              AS zoom_app_duration,
    zoom_app__end_time__c::TIMESTAMP_TZ                       AS zoom_app_end_time,
    zoom_app__join_url__c::TEXT                               AS zoom_app_join_url,
    zoom_app__number_of_participants__c::FLOAT                AS zoom_app_number_of_participants,
    zoom_app__password__c::TEXT                               AS zoom_app_password,
    zoom_app__register_url__c::TEXT                           AS zoom_app_register_url,
    zoom_app__setting_allow_multiple_devices__c::BOOLEAN      AS zoom_app_setting_allow_multiple_devices,
    zoom_app__setting_approval_type__c::TEXT                  AS zoom_app_setting_approval_type,
    zoom_app__setting_close_registration__c::BOOLEAN          AS zoom_app_setting_close_registration,
    zoom_app__setting_host_video_on__c::BOOLEAN               AS zoom_app_setting_host_video_on,
    zoom_app__setting_panelists_video_on__c::BOOLEAN          AS zoom_app_setting_panelists_video_on,
    zoom_app__setting_practice_session__c::BOOLEAN            AS zoom_app_setting_practice_session,
    zoom_app__start_time__c::TIMESTAMP_TZ                     AS zoom_app_start_time,
    zoom_app__start_url__c::TEXT                              AS zoom_app_start_url,
    zoom_app__status__c::TEXT                                 AS zoom_app_status,
    zoom_app__topic__c::TEXT                                  AS zoom_app_topic,
    zoom_app__uuid_unique__c::TEXT                            AS zoom_app_uuid_unique,
    zoom_app__uuid__c::TEXT                                   AS zoom_app_uuid,
    zoom_app__webinar_id__c::TEXT                             AS zoom_app_webinar_id,
    _sdc_batched_at::TIMESTAMP_TZ                             AS sdc_batched_at,
    _sdc_extracted_at::TIMESTAMP_TZ                           AS sdc_extracted_at,
    _sdc_received_at::TIMESTAMP_TZ                            AS sdc_received_at,
    _sdc_sequence::NUMBER                                     AS sdc_sequence,
    _sdc_table_version::NUMBER                                AS sdc_table_version,
    lastvieweddate::TIMESTAMP_TZ                              AS last_viewed_date,
    lastreferenceddate::TIMESTAMP_TZ                          AS last_referenced_date
  FROM base

)

SELECT *
FROM renamed
