{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','comments') }}
),

renamed AS (

  SELECT
    _fivetran_id::VARCHAR        AS _fivetran_id,
    _fivetran_deleted::BOOLEAN   AS _fivetran_deleted,
    comment::VARCHAR             AS comment,
    created_by_user_id::VARCHAR  AS created_by_user_id,
    author_id::VARCHAR           AS author_id,
    created_date::TIMESTAMP      AS created_date,
    visibility::VARCHAR          AS visibility,
    modified_by_user_id::VARCHAR AS modified_by_user_id,
    modified_date::TIMESTAMP     AS modified_date,
    comment_id::VARCHAR          AS comment_id,
    _fivetran_synced::TIMESTAMP  AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
