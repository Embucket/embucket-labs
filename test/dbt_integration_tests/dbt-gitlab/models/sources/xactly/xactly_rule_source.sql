WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_rule') }}

),

renamed AS (

  SELECT

    rule_id::FLOAT AS rule_id,
    version::FLOAT AS version,
    name::VARCHAR as name,
    description::VARCHAR as description,
    rule_type::FLOAT as rule_type,
    active_start_date::VARCHAR as active_start_date,
    active_end_date::VARCHAR as active_end_date,
    is_active::VARCHAR AS is_active,
    input_type::FLOAT as input_type,
    pos_relation_id::FLOAT as pos_relation_id,
    pos_relation_name::VARCHAR as pos_relation_name,
    bonus_type::FLOAT as bonus_type,
    period_type_name::VARCHAR as period_type_name,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    rollable_on_reporting::VARCHAR as rollable_on_reporting,
    hold_type::FLOAT as hold_type,
    primary_rule_id::FLOAT as primary_rule_id,
    rate_type::FLOAT as rate_type,
    attainment_measure_id::VARCHAR as attainment_measure_id,
    effective_start_date::VARCHAR as effective_start_date,
    effective_end_date::VARCHAR as effective_end_date,
    version_notes::VARCHAR as version_notes,
    is_master::VARCHAR as is_master,
    multiplier::VARCHAR as multiplier

  FROM source

)

SELECT *
FROM renamed
