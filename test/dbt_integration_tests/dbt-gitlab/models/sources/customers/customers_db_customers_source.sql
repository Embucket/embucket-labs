WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_customers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                      AS customer_id,
      first_name::VARCHAR              AS customer_first_name,
      last_name::VARCHAR               AS customer_last_name,
      email::VARCHAR                   AS customer_email,
      created_at::TIMESTAMP            AS customer_created_at,
      updated_at::TIMESTAMP            AS customer_updated_at,
      sign_in_count::NUMBER           AS sign_in_count,
      current_sign_in_at::TIMESTAMP    AS current_sign_in_at,
      last_sign_in_at::TIMESTAMP       AS last_sign_in_at,
      --current_sign_in_ip,
      --last_sign_in_ip,
      phone_number::VARCHAR            AS customer_phone_number,
      provider::VARCHAR                AS customer_provider,
      NULLIF(uid, '')::VARCHAR         AS customer_provider_user_id,
      country::VARCHAR                 AS country,
      state::VARCHAR                   AS state,
      city::VARCHAR                    AS city,
      vat_code::VARCHAR                AS vat_code,
      company::VARCHAR                 AS company,
      company_size::VARCHAR            AS company_size,
      salesforce_account_id::VARCHAR   AS sfdc_account_id,
      billable::BOOLEAN                AS customer_is_billable,
      confirmed_at::TIMESTAMP          AS confirmed_at,
      confirmation_sent_at::TIMESTAMP  AS confirmation_sent_at
    FROM source

)

SELECT *
FROM renamed
