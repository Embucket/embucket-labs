WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_subscription_user_add_on_assignment_versions_dedupe_source') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS id,
      organization_id::NUMBER                   AS organization_id,
      item_id::NUMBER                           AS item_id,
      purchase_id::NUMBER                       AS purchase_id,
      user_id::NUMBER                           AS user_id,
      created_at::TIMESTAMP                     AS created_at,
      event::VARCHAR                            AS event,
      item_type::VARCHAR                        AS item_type,
      namespace_path::VARCHAR                   AS namespace_path,
      add_on_name::VARCHAR                      AS add_on_name,
      object::VARCHAR                           AS object,
      TO_TIMESTAMP(_uploaded_at::INT)           AS uploaded_at

    FROM source

)

SELECT  *
FROM renamed
