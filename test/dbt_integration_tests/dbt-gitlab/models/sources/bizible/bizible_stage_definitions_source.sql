WITH source AS (

    SELECT
      id                        AS stage_definition_id,
      modified_date             AS modified_date,
      stage_name                AS stage_name,
      is_inactive               AS is_inactive,
      is_in_custom_model        AS is_in_custom_model,
      is_boomerang              AS is_boomerang,
      is_transition_tracking    AS is_transition_tracking,
      stage_status              AS stage_status,
      is_from_salesforce        AS is_from_salesforce,
      is_default                AS is_default,
      rank                      AS rank,
      is_deleted                AS is_deleted,
      _created_date             AS _created_date,
      _modified_date            AS _modified_date,
      _deleted_date             AS _deleted_date
    FROM {{ source('bizible', 'biz_stage_definitions') }}
 
)

SELECT *
FROM source
