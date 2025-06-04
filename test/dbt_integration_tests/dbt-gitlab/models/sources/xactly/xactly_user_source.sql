WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_user') }}

),

renamed AS (

  SELECT

    user_id::FLOAT AS user_id,
    version::FLOAT AS version,
    email::VARCHAR AS email,
    name::VARCHAR AS name,
    passwd_chg_dt::VARCHAR AS passwd_chg_dt,
    passwd_exp_dt::VARCHAR AS passwd_exp_dt,
    ip_addresses::VARCHAR AS ip_addresses,
    is_active::VARCHAR AS is_active,
    is_passwd_reset::VARCHAR AS is_passwd_reset,
    business_group_id::FLOAT AS business_group_id,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    locale::VARCHAR AS locale,
    enabled::VARCHAR AS enabled,
    accepted_contract::VARCHAR AS accepted_contract,
    source_id::FLOAT AS source_id,
    primary_role_type::VARCHAR AS primary_role_type,
    activated_date::VARCHAR AS activated_date,
    is_internal_user::VARCHAR AS is_internal_user,
    user_uuid::VARCHAR AS user_uuid


  FROM source

)

SELECT *
FROM renamed
