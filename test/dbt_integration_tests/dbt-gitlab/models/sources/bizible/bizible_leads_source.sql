WITH source AS (

    SELECT

      id                                AS lead_id,
      modified_date                     AS modified_date,
      created_date                      AS created_date,
      email                             AS email,
      web_site                          AS web_site,
      company                           AS company,
      lead_source                       AS lead_source,
      is_converted                      AS is_converted,
      converted_opportunity_id          AS converted_opportunity_id,
      converted_date                    AS converted_date,
      converted_contact_id              AS converted_contact_id,
      accountid                         AS accountid,
      bizible_stage                     AS bizible_stage,
      bizible_stage_previous            AS bizible_stage_previous,
      odds_of_conversion                AS odds_of_conversion,
      lead_score_model                  AS lead_score_model,
      lead_score_results                AS lead_score_results,
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

    FROM {{ source('bizible', 'biz_leads') }}
 
)

SELECT *
FROM source

