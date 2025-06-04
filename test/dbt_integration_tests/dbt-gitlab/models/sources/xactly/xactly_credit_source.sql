WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit') }}

), renamed AS (

    SELECT

      amount::FLOAT                                 AS amount,
      amount_display_symbol::VARCHAR                AS amount_display_symbol,
      amount_unit_type_id::FLOAT                    AS amount_unit_type_id,
      batch_number::FLOAT                           AS batch_number,
      business_group_id::FLOAT                      AS business_group_id,
      created_by_id::FLOAT                          AS created_by_id,
      created_by_name::VARCHAR                      AS created_by_name,
      created_date::VARCHAR                         AS created_date,
      credit_id::FLOAT                              AS credit_id,
      credit_type_id::FLOAT                         AS credit_type_id,
      credit_type_name::VARCHAR                     AS credit_type_name,
      customer_id::FLOAT                            AS customer_id,
      customer_name::VARCHAR                        AS customer_name,
      eff_participant_id::FLOAT                     AS eff_participant_id,
      eff_position_id::FLOAT                        AS eff_position_id,
      estimated_rel_date::VARCHAR                   AS estimated_rel_date,
      ever_on_hold::VARCHAR                         AS ever_on_hold,
      geography_id::FLOAT                           AS geography_id,
      geography_name::VARCHAR                       AS geography_name,
      incentive_date::VARCHAR                       AS incentive_date,
      is_active::VARCHAR                            AS is_active,
      is_held::VARCHAR                              AS is_held,
      is_processed::VARCHAR                         AS is_processed,
      is_rollable::VARCHAR                          AS is_rollable,
      item_code::VARCHAR                            AS item_code,
      mgr_eff_part_id::FLOAT                        AS mgr_eff_part_id,
      mgr_eff_pos_id::FLOAT                         AS mgr_eff_pos_id,
      mgr_master_pos_id::FLOAT                      AS mgr_master_pos_id,
      modified_by_id::FLOAT                         AS modified_by_id,
      modified_by_name::VARCHAR                     AS modified_by_name,
      modified_date::VARCHAR                        AS modified_date,
      name::VARCHAR                                 AS name,
      order_code::VARCHAR                           AS order_code,
      order_item_id::FLOAT                          AS order_item_id,
      participant_id::FLOAT                         AS participant_id,
      participant_name::VARCHAR                     AS participant_name,
      period_id::FLOAT                              AS period_id,
      period_name::VARCHAR                          AS period_name,
      plan_id::FLOAT                                AS plan_id,
      plan_name::VARCHAR                            AS plan_name,
      position_id::FLOAT                            AS position_id,
      position_name::VARCHAR                        AS position_name,
      product_id::FLOAT                             AS product_id,
      product_name::VARCHAR                         AS product_name,
      reason_code_id::FLOAT                         AS reason_code_id,
      reason_code_name::VARCHAR                     AS reason_code_name,
      release_date::VARCHAR                         AS release_date,
      release_group_id::FLOAT                       AS release_group_id,
      rollable_on_reporting::VARCHAR                AS rollable_on_reporting,
      rule_id::FLOAT                                AS rule_id,
      rule_name::VARCHAR                            AS rule_name,
      run_id::FLOAT                                 AS run_id,
      source_credit_id::FLOAT                       AS source_credit_id,
      source_position_id::FLOAT                     AS source_position_id,
      src_pos_relation_type_id::FLOAT               AS src_pos_relation_type_id,
      sub_batch_number::FLOAT                       AS sub_batch_number,
      sub_part_key::VARCHAR                         AS sub_part_key,
      trans_id::FLOAT                               AS trans_id

    FROM source
    
)

SELECT *
FROM renamed