WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','sla') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                   AS sys_id,
    users_value::VARCHAR              AS users_value,
    signatures::VARCHAR               AS signatures,
    technical_lead_value::VARCHAR     AS technical_lead_value,
    sys_updated_on::TIMESTAMP         AS sys_updated_on,
    change_procedures::VARCHAR        AS change_procedures,
    ends::DATE                        AS ends,
    begins::DATE                      AS begins,
    reponsibilities::VARCHAR          AS reponsibilities,
    calendar_value::VARCHAR           AS calendar_value,
    contract_value::VARCHAR           AS contract_value,
    sys_created_on::TIMESTAMP         AS sys_created_on,
    number::VARCHAR                   AS number,
    functional_area::VARCHAR          AS functional_area,
    service_goals::VARCHAR            AS service_goals,
    security_notes::VARCHAR           AS security_notes,
    responsible_user_value::VARCHAR   AS responsible_user_value,
    department_value::VARCHAR         AS department_value,
    description::VARCHAR              AS description,
    active::BOOLEAN                   AS active,
    sys_created_by::VARCHAR           AS sys_created_by,
    notes::VARCHAR                    AS notes,
    sys_updated_by::VARCHAR           AS sys_updated_by,
    sys_class_name::VARCHAR           AS sys_class_name,
    maintenance_value::VARCHAR        AS maintenance_value,
    transaction_load::FLOAT           AS transaction_load,
    name::VARCHAR                     AS name,
    business_unit::VARCHAR            AS business_unit,
    incident_procedures::VARCHAR      AS incident_procedures,
    disaster_recovery::VARCHAR        AS disaster_recovery,
    sys_mod_count::NUMBER             AS sys_mod_count,
    informed_user_value::VARCHAR      AS informed_user_value,
    short_description::VARCHAR        AS short_description,
    avail_pct::FLOAT                  AS avail_pct,
    next_review::DATE                 AS next_review,
    response_time::FLOAT              AS response_time,
    business_lead_value::VARCHAR      AS business_lead_value,
    consultant_user_value::VARCHAR    AS consultant_user_value,
    accountable_user_value::VARCHAR   AS accountable_user_value,
    _fivetran_synced::TIMESTAMP       AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN        AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed