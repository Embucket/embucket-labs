WITH source AS (

    SELECT

      id                                    AS opportunity_id,
      modified_date                         AS modified_date,
      created_date                          AS created_date,
      account_id                            AS account_id,
      name                                  AS name,
      opportunity_type                      AS opportunity_type,
      is_won                                AS is_won,
      is_closed                             AS is_closed,
      close_date                            AS close_date,
      bizible_custom_model_date             AS bizible_custom_model_date,
      amount                                AS amount,
      converted_from_lead_id                AS converted_from_lead_id,
      primary_contact_id                    AS primary_contact_id,
      odds_of_conversion                    AS odds_of_conversion,
      bizible_stage                         AS bizible_stage,
      bizible_stage_previous                AS bizible_stage_previous,
      is_deleted                            AS is_deleted,
      custom_properties                     AS custom_properties,
      currency_iso_code                     AS currency_iso_code,
      row_key                               AS row_key,
      currency_id                           AS currency_id,
      _created_date                         AS _created_date,
      _modified_date                        AS _modified_date,
      _deleted_date                         AS _deleted_date

    FROM {{ source('bizible', 'biz_opportunities') }}
 
)

SELECT *
FROM source

