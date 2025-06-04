WITH source AS (

    SELECT
    ---Other attributes
    {{ hash_sensitive_columns('customers_db_customers_source') }}
    FROM {{ ref('customers_db_customers_source') }}

)

, final AS (

    SELECT
      --Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['customer_id'])}}  AS dim_user_sk,

      --Natural Key
      customer_id                                    AS dim_user_id,

      --Other attributes
      customer_first_name_hash                       AS user_first_name,
      customer_last_name_hash                        AS user_last_name,
      customer_email_hash                            AS user_email,
      customer_created_at                            AS user_created_at,
      customer_updated_at                            AS user_updated_at,
      sign_in_count,
      current_sign_in_at,
      last_sign_in_at,
      customer_provider                              AS user_provider,
      customer_provider_user_id                      AS user_provider_user_id,

      country,
      state,
      city,
      vat_code,
      company,
      company_size,

      --Join key to CRM data
      sfdc_account_id,

      customer_is_billable                           AS is_user_billable,
      confirmed_at,
      confirmation_sent_at

    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-05-25",
    updated_date="2023-05-25"
) }}
