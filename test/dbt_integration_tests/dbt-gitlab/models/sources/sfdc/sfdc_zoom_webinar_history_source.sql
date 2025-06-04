{{ config(
    tags=["mnpi_exception"]
) }}
WITH base AS (

  SELECT *
  FROM {{ source('salesforce', 'zoom_webinar_history') }}

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
    zoom_app__contact__c::TEXT                 AS zoom_app_contact,
    zoom_app__is_attendeed__c::BOOLEAN         AS zoom_app_is_attendeed,
    zoom_app__lead__c::TEXT                    AS zoom_app_lead,
    zoom_app__webinar_topic__c::TEXT           AS zoom_app_webinar_topic,
    zoom_app__zoom_webinar_attendee__c::TEXT   AS zoom_app_zoom_webinar_attendee,
    zoom_app__zoom_webinar_panelist__c::TEXT   AS zoom_app_zoom_webinar_panelist,
    zoom_app__zoom_webinar_registrant__c::TEXT AS zoom_app_zoom_webinar_registrant,
    zoom_app__zoom_webinar__c::TEXT            AS zoom_app_zoom_webinar,
    _sdc_batched_at::TIMESTAMP_TZ              AS _sdc_batched_at,
    _sdc_extracted_at::TIMESTAMP_TZ            AS _sdc_extracted_at,
    _sdc_received_at::TIMESTAMP_TZ             AS _sdc_received_at,
    _sdc_sequence::NUMBER                      AS _sdc_sequence,
    _sdc_table_version::NUMBER                 AS _sdc_table_version,
    lastreferenceddate::TIMESTAMP_TZ           AS last_referenced_date,
    lastvieweddate::TIMESTAMP_TZ               AS last_viewed_date
  FROM base

)

SELECT *
FROM renamed
