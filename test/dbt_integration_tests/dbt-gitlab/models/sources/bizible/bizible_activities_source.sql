WITH source AS (

    SELECT
      id                            AS activities_id,
      lead_id                       AS lead_id,
      contact_id                    AS contact_id,
      activity_type_id              AS activity_type_id,
      activity_type_name            AS activity_type_name,
      start_date                    AS start_date,
      end_date                      AS end_date,
      campaign_id                   AS campaign_id,
      source_system                 AS source_system,
      created_date                  AS created_date,
      modified_date                 AS modified_date,
      is_deleted                    AS is_deleted,
      ad_form_id                    AS ad_form_id,
      _created_date                 AS _created_date,
      _modified_date                AS _modified_date,
      _deleted_date                 AS _deleted_date
    FROM {{ source('bizible', 'biz_activities') }}
 
)

SELECT *
FROM source