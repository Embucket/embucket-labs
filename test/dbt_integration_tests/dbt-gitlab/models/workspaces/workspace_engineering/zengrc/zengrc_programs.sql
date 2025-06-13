WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_programs_source') }}

)

SELECT program_code,
    program_title,
    program_type,
    program_description,
    custom_attributes,
    program_editors,
    program_id,
    program_managers,
    mapped_assessments,
    mapped_issues,
    mapped_org_groups,
    mapped_requests,
    mapped_risks,
    mapped_standards,
    mapped_systems,
    program_notes,
    program_readers,
    program_reference_url,
    program_primary_contact,
    program_secondary_contact,
    program_status,
    program_tags,
    program_url,
    program_effective_date,
    program_created_at,
    program_updated_at,
    program_stop_date,
    program_loaded_at
FROM source