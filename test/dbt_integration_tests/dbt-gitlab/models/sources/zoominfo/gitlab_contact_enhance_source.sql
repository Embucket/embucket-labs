WITH source AS (

  SELECT *
  FROM {{ source('zoominfo', 'contact_enhance') }}

),

renamed AS (

  SELECT
    "Record ID"::VARCHAR AS record_id,
    row_integer::VARCHAR AS user_id,
    first_name::VARCHAR AS first_name,
    last_name::VARCHAR AS last_name,
    users_name::VARCHAR AS users_name,
    email_id::VARCHAR AS email_id,
    internal_value1::VARCHAR AS internal_value1,
    internal_value2::VARCHAR AS internal_value2,
    company_name::VARCHAR AS company_name,
    parent_company_name::VARCHAR AS parent_company_name,
    email_type::VARCHAR AS email_type,
    "Match status"::VARCHAR AS match_status,
    "ZoomInfo Contact ID"::VARCHAR AS zoominfo_contact_id,
    "Last Name"::VARCHAR AS lastname,
    "First Name"::VARCHAR AS firstname,
    "Middle Name"::VARCHAR AS middlename,
    "Salutation"::VARCHAR AS salutation, -- noqa:L059
    "Suffix"::VARCHAR AS suffix, -- noqa:L059
    "Job Title"::VARCHAR AS job_title,
    "Job Function"::VARCHAR AS job_function,
    "Management Level"::VARCHAR AS management_level,
    "Company Division Name"::VARCHAR AS company_division_name,
    "Direct Phone Number"::VARCHAR AS direct_phone_number,
    "Email Address"::VARCHAR AS email_address,
    "Email Domain"::VARCHAR AS email_domain,
    "Department"::VARCHAR AS department, -- noqa:L059
    "Supplemental Email"::VARCHAR AS supplemental_email,
    "Mobile phone"::VARCHAR AS mobile_phone,
    "Contact Accuracy Score"::VARCHAR AS contact_accuracy_score,
    "Contact Accuracy Grade"::VARCHAR AS contact_accuracy_grade,
    "ZoomInfo Contact Profile URL"::VARCHAR AS zoominfo_contact_profile_url,
    "LinkedIn Contact Profile URL"::VARCHAR AS linkedin_contact_profile_url,
    "Notice Provided Date"::VARCHAR AS notice_provided_date,
    "Known First Name"::VARCHAR AS known_first_name,
    "Known Last Name"::VARCHAR AS known_last_name,
    "Known Full Name"::VARCHAR AS known_full_name,
    "Normalized First Name"::VARCHAR AS normalized_first_name,
    "Normalized Last Name"::VARCHAR AS normalized_last_name,
    "Email Matched Person Name"::VARCHAR AS email_matched_person_name,
    "Email Matched Company Name"::VARCHAR AS email_matched_company_name,
    "Free Email"::VARCHAR AS free_email,
    "Generic Email"::VARCHAR AS generic_email,
    "Malformed Email"::VARCHAR AS malformed_email,
    "Calculated Job Function"::VARCHAR AS calculated_job_function,
    "Calculated Management Level"::VARCHAR AS calculated_management_level,
    "Person Has Moved"::VARCHAR AS person_has_moved,
    "Person Looks Like EU"::VARCHAR AS person_looks_like_eu,
    "Within EU"::VARCHAR AS within_eu,
    "Person Street"::VARCHAR AS person_street,
    "Person City"::VARCHAR AS person_city,
    "Person State"::VARCHAR AS person_state,
    "Person Zip Code"::VARCHAR AS person_zip_code,
    "Country"::VARCHAR AS country, -- noqa:L059
    "Company Name"::VARCHAR AS companyname,
    "Website"::VARCHAR AS website, -- noqa:L059
    "Founded Year"::VARCHAR AS founded_year,
    "Company HQ Phone"::VARCHAR AS company_hq_phone,
    "Fax"::VARCHAR AS fax, -- noqa:L059
    "Ticker"::VARCHAR AS ticker, -- noqa:L059
    "Revenue (in 000s)"::VARCHAR AS revenue,
    "Revenue Range"::VARCHAR AS revenue_range,
    "Est. Marketing Department Budget (in 000s)"::VARCHAR AS est_marketing_department_budget, -- noqa:L026,L028,L016
    "Est. Finance Department Budget (in 000s)"::VARCHAR AS est_finance_department_budget, -- noqa:L026,L016
    "Est. IT Department Budget (in 000s)"::VARCHAR AS est_it_department_budget, -- noqa:L026
    "Est. HR Department Budget (in 000s)"::VARCHAR AS est_hr_department_budget, -- noqa:L026
    "Employees"::VARCHAR AS employees, -- noqa:L059
    "Employee Range"::VARCHAR AS employee_range,
    "Past 1 Year Employee Growth Rate"::VARCHAR AS past_1_year_employee_growth_rate,
    "Past 2 Year Employee Growth Rate"::VARCHAR AS past_2_year_employee_growth_rate,
    "SIC Code 1"::VARCHAR AS sic_code_1,
    "SIC Code 2"::VARCHAR AS sic_code_2,
    "SIC Codes"::VARCHAR AS sic_codes,
    "NAICS Code 1"::VARCHAR AS naics_code_1,
    "NAICS Code 2"::VARCHAR AS naics_code_2,
    "NAICS Codes"::VARCHAR AS naics_codes,
    "Primary Industry"::VARCHAR AS primary_industry,
    "Primary Sub-Industry"::VARCHAR AS primary_sub_industry,
    "All Industries"::VARCHAR AS all_industries,
    "All Sub-Industries"::VARCHAR AS all_sub_industries,
    "Industry Hierarchical Category"::VARCHAR AS industry_hierarchical_category,
    "Secondary Industry Hierarchical Category"::VARCHAR AS secondary_industry_hierarchical_category,
    "Alexa Rank"::VARCHAR AS alexa_rank,
    "ZoomInfo Company Profile URL"::VARCHAR AS zoominfo_company_profile_url,
    "LinkedIn Company Profile URL"::VARCHAR AS linkedin_company_profile_url,
    "Facebook Company Profile URL"::VARCHAR AS facebook_company_profile_url,
    "Twitter Company Profile URL"::VARCHAR AS twitter_company_profile_url,
    "Ownership Type"::VARCHAR AS ownership_type,
    "Business Model"::VARCHAR AS business_model,
    "Certified Active Company"::VARCHAR AS certified_active_company,
    "Certification Date"::VARCHAR AS certification_date,
    "Total Funding Amount (in 000s)"::VARCHAR AS total_funding_amount,
    "Recent Funding Amount (in 000s)"::VARCHAR AS recent_funding_amount,
    "Recent Funding Date"::VARCHAR AS recent_funding_date,
    "Company Street Address"::VARCHAR AS company_street_address,
    "Company City"::VARCHAR AS company_city,
    "Company State"::VARCHAR AS company_state,
    "Company Zip Code"::VARCHAR AS company_zip_code,
    "Company Country"::VARCHAR AS company_country,
    "Full Address"::VARCHAR AS full_address,
    "Number of Locations"::VARCHAR AS number_of_locations,
    IFF(
      "ZoomInfo Company ID" = '' OR "ZoomInfo Company ID" = 0,
      NULL, "ZoomInfo Company ID"
    )::VARCHAR AS zoominfo_company_id
  FROM source

)

SELECT *
FROM renamed
