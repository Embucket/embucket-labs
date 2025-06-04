WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_subscription_user_add_on_assignments_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                AS id,
      add_on_purchase_id::NUMBER                AS add_on_purchase_id,
      user_id::NUMBER                           AS user_id,
      created_at::TIMESTAMP                     AS created_at,
      updated_at::TIMESTAMP                     AS updated_at,
      pgp_is_deleted                            AS pgp_is_deleted,
      pgp_is_deleted_updated_at                 AS pgp_is_deleted_updated_at

    FROM source

)

SELECT  *
FROM renamed
