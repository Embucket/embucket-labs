WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','sys_user') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                       AS sys_id,
    sso_source::VARCHAR                   AS sso_source,
    default_perspective_value::VARCHAR    AS default_perspective_value,
    cost_center_value::VARCHAR            AS cost_center_value,
    hashed_user_id::VARCHAR               AS hashed_user_id,
    sys_created_on::TIMESTAMP             AS sys_created_on,
    active::BOOLEAN                       AS active,
    sys_created_by::VARCHAR               AS sys_created_by,
    sys_class_name::VARCHAR               AS sys_class_name,
    sys_mod_count::NUMBER                 AS sys_mod_count,
    date_format::VARCHAR                  AS date_format,
    time_format::VARCHAR                  AS time_format,
    locked_out::BOOLEAN                   AS locked_out,
    schedule_value::VARCHAR               AS schedule_value,
    manager_value::VARCHAR                AS manager_value,
    sys_updated_on::TIMESTAMP             AS sys_updated_on,
    u_gl_department::VARCHAR              AS u_gl_department,
    failed_attempts::NUMBER               AS failed_attempts,
    last_login::DATE                      AS last_login,
    sys_domain_path::VARCHAR              AS sys_domain_path,
    sys_domain_value::VARCHAR             AS sys_domain_value,
    sys_updated_by::VARCHAR               AS sys_updated_by,
    internal_integration_user::BOOLEAN    AS internal_integration_user,
    u_gl_division::VARCHAR                AS u_gl_division,
    source::VARCHAR                       AS source,
    federated_id::VARCHAR                 AS federated_id,
    accumulated_roles::VARCHAR            AS accumulated_roles,
    enable_multifactor_authn::BOOLEAN     AS enable_multifactor_authn,
    u_job_start_date::DATE                AS u_job_start_date,
    email::VARCHAR                        AS email,
    _fivetran_synced::TIMESTAMP           AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN            AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed