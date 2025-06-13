WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'amendment') }}

), renamed AS (

    SELECT
      --keys
      id::VARCHAR                               AS amendment_id,
      subscription_id::VARCHAR                  AS subscription_id,
      code::VARCHAR                             AS amendment_code,
      status::VARCHAR                           AS amendment_status,
      name::VARCHAR                             AS amendment_name,
      service_activation_date::TIMESTAMP_TZ     AS service_activation_date,
      current_term::NUMBER                      AS current_term,
      description::VARCHAR                      AS amendment_description,
      current_term_period_type::VARCHAR         AS current_term_period_type,
      customer_acceptance_date::TIMESTAMP_TZ    AS customer_acceptance_date,
      effective_date::TIMESTAMP_TZ              AS effective_date,
      renewal_setting::VARCHAR                  AS renewal_setting,
      term_start_date::TIMESTAMP_TZ             AS term_start_date,
      contract_effective_date::TIMESTAMP_TZ     AS contract_effective_date,
      type::VARCHAR                             AS amendment_type,
      auto_renew::BOOLEAN                       AS auto_renew,
      renewal_term_period_type::VARCHAR         AS renewal_term_period_type,
      renewal_term::NUMBER                      AS renewal_term,
      term_type::VARCHAR                        AS term_type,

      -- metadata
      created_by_id::VARCHAR                    AS created_by_id,
      created_date::TIMESTAMP_TZ                AS created_date,
      updated_by_id::VARCHAR                    AS updated_by_id,
      updated_date::TIMESTAMP_TZ                AS updated_date,
      _FIVETRAN_DELETED::BOOLEAN                AS is_deleted,
      _FIVETRAN_SYNCED::TIMESTAMP_TZ            AS _FIVETRAN_SYNCED

    FROM source
)

SELECT *
FROM renamed