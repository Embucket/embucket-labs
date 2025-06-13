WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','sys_user_group') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                   AS sys_id,
    sys_updated_by::VARCHAR           AS sys_updated_by,
    default_assignee_value::VARCHAR   AS default_assignee_value,
    name::VARCHAR                     AS name,
    manager_value::VARCHAR            AS manager_value,
    include_members::BOOLEAN          AS include_members,
    sys_updated_on::TIMESTAMP         AS sys_updated_on,
    sys_mod_count::NUMBER             AS sys_mod_count,
    cost_center_value::VARCHAR        AS cost_center_value,
    source::VARCHAR                   AS source,
    sys_created_on::TIMESTAMP         AS sys_created_on,
    exclude_manager::BOOLEAN          AS exclude_manager,
    type::VARCHAR                     AS type,
    description::VARCHAR              AS description,
    parent_value::VARCHAR             AS parent_value,
    sys_created_by::VARCHAR           AS sys_created_by,
    active::BOOLEAN                   AS active,
    _fivetran_synced::TIMESTAMP       AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN        AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed