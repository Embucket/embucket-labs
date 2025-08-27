{{ config({
    "alias": "customers_db_licenses_snapshots",
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'customers_db_licenses_snapshots') }}

), renamed AS (

    SELECT DISTINCT
      dbt_scd_id::VARCHAR                        AS license_snapshot_id,
      id::NUMBER                                 AS license_id,
      company::VARCHAR                           AS company,
      users_count::NUMBER                        AS users_count,
      REPLACE(license_file_md5::VARCHAR, '-')    AS license_md5,
      expires_at::TIMESTAMP                      AS license_expires_at,
      plan_name::VARCHAR                         AS plan_name,
      starts_at::TIMESTAMP                       AS starts_at,
      zuora_subscription_id::VARCHAR             AS zuora_subscription_id,
      previous_users_count::NUMBER               AS previous_users_count,
      trueup_quantity::NUMBER                    AS trueup_quantity,
      trueup_from::TIMESTAMP                     AS trueup_from,
      trueup_to::TIMESTAMP                       AS trueup_to,
      plan_code::VARCHAR                         AS plan_code,
      trial::BOOLEAN                             AS is_trial,
      created_at::TIMESTAMP                      AS created_at,
      updated_at::TIMESTAMP                      AS updated_at,
      dbt_valid_from::TIMESTAMP                AS valid_from,
      dbt_valid_to::TIMESTAMP                  AS valid_to,
      REPLACE(license_file_sha256::VARCHAR, '-') AS license_sha256
    FROM source

)

SELECT *
FROM renamed
