WITH source AS (

  SELECT *
  FROM {{ source('customers', 'customers_db_trial_histories') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY gl_namespace_id, trial_type ORDER BY updated_at DESC) = 1

),

renamed AS (

  SELECT DISTINCT
    TRY_TO_NUMERIC(gl_namespace_id)::VARCHAR AS gl_namespace_id,
    start_date::TIMESTAMP                    AS start_date,
    expired_on::TIMESTAMP                    AS expired_on,
    created_at::TIMESTAMP                    AS created_at,
    updated_at::TIMESTAMP                    AS updated_at,
    glm_source::VARCHAR                      AS glm_source,
    glm_content::VARCHAR                     AS glm_content,
    trial_type::INTEGER                      AS trial_type,
    CASE
      WHEN trial_type = 1 THEN 'Ultimate/Premium'
      WHEN trial_type = 2 THEN 'DuoPro'
      WHEN trial_type IN (3,5,6) THEN 'Duo Enterprise'
      WHEN trial_type = 4 THEN 'Ultimate on Premium'
      WHEN trial_type = 7 THEN 'Premium'
    END                                      AS trial_type_name,
    product_rate_plan_id
  FROM source

)

SELECT *
FROM renamed
WHERE gl_namespace_id IS NOT NULL
