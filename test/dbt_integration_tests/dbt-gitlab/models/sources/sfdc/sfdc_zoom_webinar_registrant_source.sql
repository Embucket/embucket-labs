{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

  SELECT *
  FROM {{ source('salesforce', 'zoom_webinar_registrant') }}
),

renamed AS (

  SELECT
    createdbyid::TEXT                AS created_by_id,
    createddate::TIMESTAMP_TZ        AS created_date,
    id::TEXT                         AS id,
    isdeleted::BOOLEAN               AS is_deleted,
    lastmodifiedbyid::TEXT           AS last_modified_by_id,
    lastmodifieddate::TIMESTAMP_TZ   AS last_modified_date,
    name::TEXT                       AS name,
    ownerid::TEXT                    AS owner_id,
    systemmodstamp::TIMESTAMP_TZ     AS system_mod_stamp,
    zoom_app__email__c::TEXT         AS zoom_app_email,
    zoom_app__external_key__c::TEXT  AS zoom_app_external_key,
    zoom_app__first_name__c::TEXT    AS zoom_app_first_name,
    zoom_app__job_title__c::TEXT     AS zoom_app_job_title,
    zoom_app__join_url1__c::TEXT     AS zoom_app_join_url1,
    zoom_app__last_name__c::TEXT     AS zoom_app_last_name,
    zoom_app__org__c::TEXT           AS zoom_app_org,
    zoom_app__status__c::TEXT        AS zoom_app_status,
    zoom_app__zoom_webinar__c::TEXT  AS zoom_app_zoom_webinar,
    _sdc_batched_at::TIMESTAMP_TZ    AS _sdc_batched_at,
    _sdc_extracted_at::TIMESTAMP_TZ  AS _sdc_extracted_at,
    _sdc_received_at::TIMESTAMP_TZ   AS _sdc_received_at,
    _sdc_sequence::NUMBER            AS _sdc_sequence,
    _sdc_table_version::NUMBER       AS _sdc_table_version,
    lastreferenceddate::TIMESTAMP_TZ AS last_referenced_date,
    lastvieweddate::TIMESTAMP_TZ     AS last_viewed_date
  FROM base

)

SELECT *
FROM renamed
