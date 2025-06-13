WITH source AS (

    SELECT {{ hash_sensitive_columns('customers_db_leads_source') }}
    FROM {{ ref('customers_db_leads_source') }}

), final AS (

    SELECT

      --Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['leads_id'])}}  AS dim_lead_sk,
      
      --Natural Key(CDot System-generated)
      leads_id                                    AS internal_lead_id,
      
      --Foreign Keys                           
      namespace_id                                AS dim_namespace_id,
      user_id,

      first_name_hash,
      last_name_hash,
      email_hash,
      phone_hash,
      company_name_hash,
      created_at,
      updated_at,
      trial_start_date,
      opt_in::BOOLEAN                                AS is_opt_in,
      currently_in_trial::BOOLEAN                    AS is_currently_in_trial,
      is_for_business_use::BOOLEAN                      AS is_for_business_use,
      employees_bucket,
      country,
      state,
      product_interaction,
      provider,
      comment_capture,
      glm_content,
      glm_source,
      sent_at,
      website_url,
      role,
      jtbd                                      AS jobs_to_be_done
    
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-07-26",
    updated_date="2023-07-26"
) }}