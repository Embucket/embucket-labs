WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_vendors_source') }}

)
SELECT vendor_code,
    vendor_title,
    vendor_type,
    vendor_description,
    vendor_id,
    vendor_notes,
    vendor_reference_url,
    vendor_risk_rating,
    vendor_secondary_contact,
    vendor_status,
    vendor_tags,
    vendor_url,
    mapped_data_assets,
    mapped_facilities,
    mapped_products,
    mapped_systems,
    mapped_tasks,
    third_party_type,
    vendor_security_contact,
    critical_system_tier,
    data_classification,
    processor_controller,
    collects_personal_information,
    third_party_residual_risk,
    third_party_inherent_risk,
    vendor_start_date,
    vendor_created_at,
    vendor_updated_at,
    vendor_stop_date,
    vendor_loaded_at
FROM source