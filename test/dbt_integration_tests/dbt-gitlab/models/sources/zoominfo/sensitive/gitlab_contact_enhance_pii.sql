  WITH source AS (

    SELECT 
      first_name,
      last_name,
      users_name,
      email_id,
      lastname,
      firstname,
      middlename,
      direct_phone_number,
      email_address,
      supplemental_email,
      mobile_phone,
      zoominfo_contact_profile_url,
      linkedin_contact_profile_url,
      normalized_first_name,
      normalized_last_name,
      person_street,
      person_city,
      person_state,
      person_zip_code,
      {{hash_sensitive_columns('gitlab_contact_enhance_source') }}
    FROM {{ ref('gitlab_contact_enhance_source') }}

)

SELECT *
FROM source




 
