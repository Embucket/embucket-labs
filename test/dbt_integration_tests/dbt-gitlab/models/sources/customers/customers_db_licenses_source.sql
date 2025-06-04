WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_licenses') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                 AS license_id,
      name::VARCHAR                              AS name,
      company::VARCHAR                           AS company,
      email::VARCHAR                             AS email,
      users_count::NUMBER                        AS license_user_count,
      license_file::VARCHAR                      AS license_file,
      expires_at::DATE                           AS license_expire_date,
      created_at::TIMESTAMP                      AS created_at,
      updated_at::TIMESTAMP                      AS updated_at,
      plan_name::VARCHAR                         AS plan_name,
      starts_at::DATE                            AS license_start_date,
      zuora_subscription_id::VARCHAR             AS zuora_subscription_id,
      notes::VARCHAR                             AS license_notes,
      previous_users_count::NUMBER               AS previous_users_count,
      trueup_quantity::NUMBER                    AS trueup_quantity,
      trueup_from::DATE                          AS trueup_from_date,
      trueup_to::DATE                            AS trueup_to_date,
      plan_code::VARCHAR                         AS plan_code,
      trial::BOOLEAN                             AS is_trial,
      zuora_subscription_name::VARCHAR           AS zuora_subscription_name,
      REPLACE(license_file_md5::VARCHAR, '-')    AS license_md5,
      REPLACE(license_file_sha256::VARCHAR, '-') AS license_sha256,
      creator_id::NUMBER                         AS creator_id
    FROM source

)

SELECT *
FROM renamed
