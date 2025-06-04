WITH source AS (
    SELECT *
    FROM {{ source('zuora', 'amendment') }}

), renamed AS (

    SELECT
      --keys
      id::VARCHAR                          AS amendment_id,
      subscriptionid::VARCHAR              AS subscription_id,
      code::VARCHAR                        AS amendment_code,
      status::VARCHAR                      AS amendment_status,
      name::VARCHAR                        AS amendment_name,
      serviceactivationdate::TIMESTAMP_TZ  AS service_activation_date,
      currentterm::NUMBER                  AS current_term,
      description::VARCHAR                 AS amendment_description,
      currenttermperiodtype::VARCHAR       AS current_term_period_type,
      customeracceptancedate::TIMESTAMP_TZ AS customer_acceptance_date,
      effectivedate::TIMESTAMP_TZ          AS effective_date,
      renewalsetting::VARCHAR              AS renewal_setting,
      termstartdate::TIMESTAMP_TZ          AS term_start_date,
      contracteffectivedate::TIMESTAMP_TZ  AS contract_effective_date,
      type::VARCHAR                        AS amendment_type,
      autorenew::BOOLEAN                   AS auto_renew,
      renewaltermperiodtype::VARCHAR       AS renewal_term_period_type,
      renewalterm::NUMBER                  AS renewal_term,
      termtype::VARCHAR                    AS term_type,

      -- metadata
      createdbyid::VARCHAR                 AS created_by_id,
      createddate::TIMESTAMP_TZ            AS created_date,
      updatedbyid::VARCHAR                 AS updated_by_id,
      updateddate::TIMESTAMP_TZ            AS updated_date,
      deleted::BOOLEAN                     AS is_deleted,
      _sdc_table_version::NUMBER           AS sdc_table_version,
      _sdc_received_at::TIMESTAMP_TZ       AS sdc_received_at,
      _sdc_sequence::NUMBER                AS sdc_sequence,
      _sdc_batched_at::TIMESTAMP_TZ        AS sdc_batched_at,
      _sdc_extracted_at::TIMESTAMP_TZ      AS sdc_extracted_at

    FROM source
)

SELECT *
FROM renamed