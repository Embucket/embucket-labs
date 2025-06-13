{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

  SELECT *
  FROM {{ source('salesforce', 'zoom_webinar_attendee') }}
),

renamed AS (
  SELECT
    createdbyid::TEXT                          AS created_by_id,
    createddate::TIMESTAMP_TZ                  AS created_date,
    id::TEXT                                   AS id,
    isdeleted::BOOLEAN                         AS is_deleted,
    lastmodifiedbyid::TEXT                     AS last_modified_by_id,
    lastmodifieddate::TIMESTAMP_TZ             AS last_modified_date,
    name::TEXT                                 AS name,
    ownerid::TEXT                              AS owner_id,
    systemmodstamp::TIMESTAMP_TZ               AS system_mod_stamp,
    zoom_app__duration_hh_mi_ss__c::TEXT       AS zoom_app_duration_hh_mi_ss,
    zoom_app__duration__c::FLOAT               AS zoom_app_duration,
    zoom_app__join_time__c::TIMESTAMP_TZ       AS zoom_app_join_time,
    zoom_app__leave_time__c::TIMESTAMP_TZ      AS zoom_app_leave_time,
    zoom_app__name__c::TEXT                    AS zoom_app_name,
    zoom_app__user_email__c::TEXT              AS zoom_app_user_email,
    zoom_app__user_id__c::TEXT                 AS zoom_app_user_id,
    zoom_app__uuid__c::TEXT                    AS zoom_app_uuid,
    zoom_app__zoom_webinar_panelist__c::TEXT   AS zoom_app_zoom_webinar_panelist,
    zoom_app__zoom_webinar_registrant__c::TEXT AS zoom_app_zoom_webinar_registrant,
    zoom_app__zoom_webinar__c::TEXT            AS zoom_app_zoom_webinar,
    _sdc_batched_at::TIMESTAMP_TZ              AS sdc_batched_at,
    _sdc_extracted_at::TIMESTAMP_TZ            AS _sdc_extracted_at,
    _sdc_received_at::TIMESTAMP_TZ             AS _sdc_received_at,
    _sdc_sequence::NUMBER                      AS _sdc_sequence,
    _sdc_table_version::NUMBER                 AS _sdc_table_version

  FROM base
)

SELECT *
FROM renamed
