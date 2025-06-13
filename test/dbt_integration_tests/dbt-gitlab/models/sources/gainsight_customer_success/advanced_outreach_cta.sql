{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','advanced_outreach_cta') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR               AS gsid,
    _fivetran_deleted::BOOLEAN  AS _fivetran_deleted,
    account::VARCHAR            AS account,
    action_type::VARCHAR        AS action_type,
    is_name_identifier::BOOLEAN AS is_name_identifier,
    created_at::TIMESTAMP       AS created_at,
    name::VARCHAR               AS name,
    area_name::VARCHAR          AS area_name,
    entity_type::VARCHAR        AS entity_type,
    modified_at::TIMESTAMP      AS modified_at,
    ctastatus::VARCHAR          AS ctastatus,
    deleted::VARCHAR            AS deleted,
    ctaid::VARCHAR              AS ctaid,
    _fivetran_synced::TIMESTAMP AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed