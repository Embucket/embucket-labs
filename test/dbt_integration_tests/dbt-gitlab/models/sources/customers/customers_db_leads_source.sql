WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_leads') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                      AS leads_id,
      created_at::TIMESTAMP           AS created_at,
      updated_at::TIMESTAMP           AS updated_at,
      trial_start_date::TIMESTAMP     AS trial_start_date,
      namespace_id::NUMBER            AS namespace_id,
      user_id::NUMBER                 AS user_id,
      opt_in::BOOLEAN                 AS opt_in,
      currently_in_trial::BOOLEAN     AS currently_in_trial,
      is_for_business_use::BOOLEAN    AS is_for_business_use,
      first_name::VARCHAR             AS first_name,
      last_name::VARCHAR              AS last_name,
      email::VARCHAR                  AS email,
      phone::VARCHAR                  AS phone,
      company_name::VARCHAR           AS company_name,
      employees_bucket::VARCHAR       AS employees_bucket,
      country::VARCHAR                AS country,
      state::VARCHAR                  AS state,
      product_interaction::VARCHAR    AS product_interaction,
      provider::VARCHAR               AS provider,
      comment_capture::VARCHAR        AS comment_capture,
      glm_content::VARCHAR            AS glm_content,
      glm_source::VARCHAR             AS glm_source,
      sent_at::TIMESTAMP              AS sent_at,
      website_url::VARCHAR            AS website_url,
      role::VARCHAR                   AS role,
      jtbd::VARCHAR                   AS jtbd      

    FROM source  

)

SELECT *
FROM renamed
