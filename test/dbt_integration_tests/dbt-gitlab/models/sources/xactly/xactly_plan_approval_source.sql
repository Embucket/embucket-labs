WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_plan_approval') }}

),

renamed AS (

  SELECT

    plan_approval_id::FLOAT AS planrules_id,
    version::FLOAT AS version,
    is_active::VARCHAR AS is_active,
    status::VARCHAR as status,
    person_id::FLOAT as person_id,
    manager_id::FLOAT as manager_id,
    plan_id::FLOAT as plan_id,
    plan_name::VARCHAR as plan_name,
    route_date::VARCHAR as route_date,
    per_acpt_date::VARCHAR as per_acpt_date,
    mgr_acpt_date::VARCHAR as mgr_acpt_date,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    doc_type::FLOAT as doc_type,
    business_group_id::FLOAT as business_group_id,
    person_name::VARCHAR as person_name,
    manager_name::VARCHAR as manager_name,
    effective_plan_date::VARCHAR as effective_plan_date,
    route_id::FLOAT as route_id,
    doc_template_id::FLOAT as doc_template_id

  FROM source

)

SELECT *
FROM renamed
