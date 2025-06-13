WITH source AS (

    SELECT

      id                          AS opp_stage_transition_id,
      account_id                  AS account_id,
      opportunity_id              AS opportunity_id,
      contact_id                  AS contact_id,
      email                       AS email,
      touchpoint_id               AS touchpoint_id,
      transition_date             AS transition_date,
      stage_id                    AS stage_id,
      stage                       AS stage,
      rank                        AS rank,
      index                       AS index,
      last_index                  AS last_index,
      is_pending                  AS is_pending,
      is_non_transitional         AS is_non_transitional,
      previous_stage_date         AS previous_stage_date,
      next_stage_date             AS next_stage_date,
      modified_date               AS modified_date,
      is_deleted                  AS is_deleted,
      _created_date               AS _created_date,
      _modified_date              AS _modified_date,
      _deleted_date               AS _deleted_date

    FROM {{ source('bizible', 'biz_opp_stage_transitions') }}
 
)

SELECT *
FROM source

