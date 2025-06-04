WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','organization') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    id::VARCHAR                                   AS id,
    organization_code::VARCHAR                    AS organization_code,
    name::VARCHAR                                 AS name,
    description::VARCHAR                          AS description,
    include_manager_in_name::BOOLEAN              AS include_manager_in_name,
    include_organization_code_in_name::BOOLEAN    AS include_organization_code_in_name,
    type::VARCHAR                                 AS type,
    sub_type::VARCHAR                             AS sub_type,
    availability_date::TIMESTAMP                  AS availability_date,
    last_updated_date_time::TIMESTAMP             AS last_updated_date_time,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE)       AS is_active,
    inactive_date::DATE                           AS inactive_date,
    manager_id::VARCHAR                           AS manager_id,
    organization_owner_id::VARCHAR                AS organization_owner_id,
    visibility::VARCHAR                           AS visibility,
    external_url::VARCHAR                         AS external_url,
    top_level_organization_id::VARCHAR            AS top_level_organization_id,
    superior_organization_id::VARCHAR             AS superior_organization_id,
    staffing_model::VARCHAR                       AS staffing_model,
    location::VARCHAR                             AS location,
    available_for_hire::BOOLEAN                   AS available_for_hire,
    hiring_freeze::BOOLEAN                        AS hiring_freeze,
    supervisory_position_availability_date::DATE  AS supervisory_position_availability_date,
    supervisory_position_earliest_hire_date::DATE AS supervisory_position_earliest_hire_date,
    supervisory_position_worker_type::VARCHAR     AS supervisory_position_worker_type,
    supervisory_position_time_type::VARCHAR       AS supervisory_position_time_type,
    code::VARCHAR                                 AS code,
    _fivetran_deleted::BOOLEAN                    AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP                   AS _fivetran_synced
  FROM source

)

SELECT *
FROM final
