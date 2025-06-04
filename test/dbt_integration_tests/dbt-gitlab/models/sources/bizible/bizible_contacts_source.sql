WITH source AS (

    SELECT
      id                                AS contact_id,
      modified_date                     AS modified_date,
      created_date                      AS created_date,
      email                             AS email,
      accountid                         AS accountid,
      lead_source                       AS lead_source,
      job_title                         AS job_title,
      bizible_stage                     AS bizible_stage,
      bizible_stage_previous            AS bizible_stage_previous,
      odds_of_conversion                AS odds_of_conversion,
      bizible_cookie_id                 AS bizible_cookie_id,
      is_deleted                        AS is_deleted,
      is_duplicate                      AS is_duplicate,
      source_system                     AS source_system,
      other_system_id                   AS other_system_id,
      custom_properties                 AS custom_properties,
      row_key                           AS row_key,
      _created_date                     AS _created_date,
      _modified_date                    AS _modified_date,
      _deleted_date                     AS _deleted_date
    FROM {{ source('bizible', 'biz_contacts') }}
 
)

SELECT *
FROM source

