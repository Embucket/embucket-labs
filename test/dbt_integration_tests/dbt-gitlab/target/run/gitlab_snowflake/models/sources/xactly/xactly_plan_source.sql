
  
    

create or replace transient table EMBUCKET.legacy.xactly_plan_source
    

    
    as (WITH source AS (

  SELECT *
  FROM EMBUCKET.tap_xactly.xc_plan

),

renamed AS (

  SELECT

    plan_id::FLOAT AS plan_id,
    version::FLOAT AS version,
    name::VARCHAR AS name,
    description::VARCHAR AS description,
    is_active::VARCHAR AS is_active,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    period_id::FLOAT AS period_id

  FROM source

)

SELECT *
FROM renamed
    )
;


  