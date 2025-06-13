WITH source AS (

    SELECT *
    FROM {{ source('snapshots','customers_db_leads_snapshots') }}

), renamed AS (

    SELECT
      id::NUMBER AS leads_id,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at,
      trial_start_date::TIMESTAMP AS trial_start_date,
      namespace_id::NUMBER AS namespace_id,
      user_id::NUMBER AS user_id,
      opt_in::BOOLEAN AS opt_in,
      currently_in_trial::BOOLEAN AS currently_in_trial,
      is_for_business_use::BOOLEAN AS is_for_business_use,
      first_name::VARCHAR AS first_name,
      last_name::VARCHAR AS last_name,
      email::VARCHAR AS email,
      phone::VARCHAR AS phone,
      company_name::VARCHAR AS company_name,
      employees_bucket::VARCHAR AS employees_bucket,
      country::VARCHAR AS country,
      state::VARCHAR AS state,
      product_interaction::VARCHAR AS product_interaction,
      provider::VARCHAR AS provider,
      comment_capture::VARCHAR AS comment_capture,
      glm_content::VARCHAR AS glm_content,
      glm_source::VARCHAR AS glm_source,
      sent_at::TIMESTAMP AS sent_at,
      website_url::VARCHAR AS website_url,
      role::VARCHAR AS role,
      jtbd::VARCHAR AS jtbd,
      dbt_valid_from::TIMESTAMP AS dbt_valid_from,
      dbt_valid_to::TIMESTAMP AS dbt_valid_to 
    FROM source
    
)

SELECT  *
FROM renamed