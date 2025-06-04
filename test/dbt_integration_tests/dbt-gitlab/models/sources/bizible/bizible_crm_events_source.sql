WITH source AS (

    SELECT
      id                        AS crm_event_id,
      created_date              AS created_date,
      modified_date             AS modified_date,
      lead_id                   AS lead_id,
      lead_email                AS lead_email,
      contact_id                AS contact_id,
      contact_email             AS contact_email,
      bizible_cookie_id         AS bizible_cookie_id,
      activity_type             AS activity_type,
      event_start_date          AS event_start_date,
      event_end_date            AS event_end_date,
      is_deleted                AS is_deleted,
      custom_properties         AS custom_properties,
      _created_date             AS _created_date,
      _modified_date            AS _modified_date,
      _deleted_date             AS _deleted_date

    FROM {{ source('bizible', 'biz_crm_events') }}
 
)

SELECT *
FROM source

