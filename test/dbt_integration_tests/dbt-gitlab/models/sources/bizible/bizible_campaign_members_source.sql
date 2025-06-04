WITH source AS (

    SELECT

      id                            AS campaign_member_id,
      modified_date                 AS modified_date,
      created_date                  AS created_date,
      bizible_touch_point_date      AS bizible_touch_point_date,
      lead_id                       AS lead_id,
      lead_email                    AS lead_email,
      contact_id                    AS contact_id,
      contact_email                 AS contact_email,
      status                        AS status,
      has_responded                 AS has_responded,
      first_responded_date          AS first_responded_date,
      campaign_name                 AS campaign_name,
      campaign_id                   AS campaign_id,
      campaign_type                 AS campaign_type,
      campaign_sync_type            AS campaign_sync_type,
      lead_sync_status              AS lead_sync_status,
      contact_sync_status           AS contact_sync_status,
      opp_sync_status               AS opp_sync_status,
      is_deleted                    AS is_deleted,
      custom_properties             AS custom_properties,
      _created_date                 AS _created_date,
      _modified_date                AS _modified_date,
      _deleted_date                 AS _deleted_date

    FROM {{ source('bizible', 'biz_campaign_members') }}
 
)

SELECT *
FROM source

