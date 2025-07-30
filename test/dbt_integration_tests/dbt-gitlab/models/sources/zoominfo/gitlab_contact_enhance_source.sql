WITH source AS (

  SELECT *
  FROM {{ source('zoominfo', 'contact_enhance') }}

),

renamed AS (

  SELECT
    record_id::VARCHAR AS record_id,
    user_id::VARCHAR AS user_id,
    first_name::VARCHAR AS first_name,
    last_name::VARCHAR AS last_name,
    users_name::VARCHAR AS users_name,
    email_id::VARCHAR AS email_id,
    internal_value1::VARCHAR AS internal_value1,
    internal_value2::VARCHAR AS internal_value2,
    company_name::VARCHAR AS company_name,
    parent_company_name::VARCHAR AS parent_company_name,
    email_type::VARCHAR AS email_type,
    match_status::VARCHAR AS match_status,
    zoominfo_contact_id::VARCHAR AS zoominfo_contact_id,
    lastname::VARCHAR AS lastname,
    firstname::VARCHAR AS firstname,
    middlename::VARCHAR AS middlename,
    salutation::VARCHAR AS salutation, -- noqa:L059
    suffix::VARCHAR AS suffix, -- noqa:L059
    job_title::VARCHAR AS job_title,
    job_function::VARCHAR AS job_function,
    management_level::VARCHAR AS management_level,
    company_division_name::VARCHAR AS company_division_name,
    direct_phone_number::VARCHAR AS direct_phone_number,
    email_address::VARCHAR AS email_address,
    email_domain::VARCHAR AS email_domain,
    department::VARCHAR AS department, -- noqa:L059
    supplemental_email::VARCHAR AS supplemental_email,
    mobile_phone::VARCHAR AS mobile_phone,
    contact_accuracy_score::VARCHAR AS contact_accuracy_score,
    contact_accuracy_grade::VARCHAR AS contact_accuracy_grade,
    zoominfo_contact_profile_url::VARCHAR AS zoominfo_contact_profile_url,
    linkedin_contact_profile_url::VARCHAR AS linkedin_contact_profile_url,
    notice_provided_date::VARCHAR AS notice_provided_date,
    known_first_name::VARCHAR AS known_first_name,
    known_last_name::VARCHAR AS known_last_name,
    known_full_name::VARCHAR AS known_full_name,
    normalized_first_name::VARCHAR AS normalized_first_name,
    normalized_last_name::VARCHAR AS normalized_last_name,
    email_matched_person_name::VARCHAR AS email_matched_person_name,
    email_matched_company_name::VARCHAR AS email_matched_company_name,
    free_email::VARCHAR AS free_email,
    generic_email::VARCHAR AS generic_email,
    malformed_email::VARCHAR AS malformed_email,
    calculated_job_function::VARCHAR AS calculated_job_function,
    calculated_management_level::VARCHAR AS calculated_management_level,
    person_has_moved::VARCHAR AS person_has_moved,
    person_looks_like_eu::VARCHAR AS person_looks_like_eu,
    within_eu::VARCHAR AS within_eu,
    person_street::VARCHAR AS person_street,
    person_city::VARCHAR AS person_city,
    person_state::VARCHAR AS person_state,
    person_zip_code::VARCHAR AS person_zip_code,
    country::VARCHAR AS country, -- noqa:L059
    companyname::VARCHAR AS companyname,
    website::VARCHAR AS website, -- noqa:L059
    founded_year::VARCHAR AS founded_year,
    company_hq_phone::VARCHAR AS company_hq_phone,
    fax::VARCHAR AS fax, -- noqa:L059
    ticker::VARCHAR AS ticker, -- noqa:L059
    revenue::VARCHAR AS revenue,
    revenue_range::VARCHAR AS revenue_range,
    est_marketing_department_budget::VARCHAR AS est_marketing_department_budget, -- noqa:L026,L028,L016
    est_finance_department_budget::VARCHAR AS est_finance_department_budget, -- noqa:L026,L016
    est_it_department_budget::VARCHAR AS est_it_department_budget, -- noqa:L026
    est_hr_department_budget::VARCHAR AS est_hr_department_budget, -- noqa:L026
    employees::VARCHAR AS employees, -- noqa:L059
    employee_range::VARCHAR AS employee_range,
    past_1_year_employee_growth_rate::VARCHAR AS past_1_year_employee_growth_rate,
    past_2_year_employee_growth_rate::VARCHAR AS past_2_year_employee_growth_rate,
    sic_code_1::VARCHAR AS sic_code_1,
    sic_code_2::VARCHAR AS sic_code_2,
    sic_codes::VARCHAR AS sic_codes,
    naics_code_1::VARCHAR AS naics_code_1,
    naics_code_2::VARCHAR AS naics_code_2,
    naics_codes::VARCHAR AS naics_codes,
    primary_industry::VARCHAR AS primary_industry,
    primary_sub_industry::VARCHAR AS primary_sub_industry,
    all_industries::VARCHAR AS all_industries,
    all_sub_industries::VARCHAR AS all_sub_industries,
    industry_hierarchical_category::VARCHAR AS industry_hierarchical_category,
    secondary_industry_hierarchical_category::VARCHAR AS secondary_industry_hierarchical_category,
    alexa_rank::VARCHAR AS alexa_rank,
    zoominfo_company_profile_url::VARCHAR AS zoominfo_company_profile_url,
    linkedin_company_profile_url::VARCHAR AS linkedin_company_profile_url,
    facebook_company_profile_url::VARCHAR AS facebook_company_profile_url,
    twitter_company_profile_url::VARCHAR AS twitter_company_profile_url,
    ownership_type::VARCHAR AS ownership_type,
    business_model::VARCHAR AS business_model,
    certified_active_company::VARCHAR AS certified_active_company,
    certification_date::VARCHAR AS certification_date,
    total_funding_amount::VARCHAR AS total_funding_amount,
    recent_funding_amount::VARCHAR AS recent_funding_amount,
    recent_funding_date::VARCHAR AS recent_funding_date,
    company_street_address::VARCHAR AS company_street_address,
    company_city::VARCHAR AS company_city,
    company_state::VARCHAR AS company_state,
    company_zip_code::VARCHAR AS company_zip_code,
    company_country::VARCHAR AS company_country,
    full_address::VARCHAR AS full_address,
    number_of_locations::VARCHAR AS number_of_locations,
    Null as zoominfo_company_id
  FROM source

)

SELECT *
FROM renamed
