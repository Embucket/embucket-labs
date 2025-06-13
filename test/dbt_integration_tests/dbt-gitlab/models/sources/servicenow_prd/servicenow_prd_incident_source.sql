WITH source AS (
  SELECT *
  FROM {{ source('servicenow_prd','incident') }}
),

renamed AS (
  SELECT
    sys_id::VARCHAR                   AS sys_id,
    rfc_value::VARCHAR                AS rfc_value,
    reopened_time::TIMESTAMP          AS reopened_time,
    severity::NUMBER                  AS severity,
    caller_id_value::VARCHAR          AS caller_id_value,
    parent_incident_value::VARCHAR    AS parent_incident_value,
    sys_updated_on::TIMESTAMP         AS sys_updated_on,
    incident_state::NUMBER            AS incident_state,
    close_code::VARCHAR               AS close_code,
    calendar_stc::NUMBER              AS calendar_stc,
    resolved_at::TIMESTAMP            AS resolved_at,
    u_vendor::VARCHAR                 AS u_vendor,
    origin_table::VARCHAR             AS origin_table,
    resolved_by_value::VARCHAR        AS resolved_by_value,
    u_on_behalf_of_value::VARCHAR     AS u_on_behalf_of_value,
    sys_created_on::TIMESTAMP         AS sys_created_on,
    problem_id_value::VARCHAR         AS problem_id_value,
    child_incidents::NUMBER           AS child_incidents,
    reopen_count::NUMBER              AS reopen_count,
    caused_by_value::VARCHAR          AS caused_by_value,
    subcategory::VARCHAR              AS subcategory,
    business_stc::NUMBER              AS business_stc,
    hold_reason::NUMBER               AS hold_reason,
    u_vendor_ticket_number::VARCHAR   AS u_vendor_ticket_number,
    reopened_by_value::VARCHAR        AS reopened_by_value,
    _fivetran_synced::TIMESTAMP       AS _fivetran_synced,
    _fivetran_deleted::BOOLEAN        AS _fivetran_deleted
  FROM source
)

SELECT * FROM renamed