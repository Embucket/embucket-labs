 WITH source AS(
 SELECT *
  FROM {{ source('coupa', 'commodity') }}
  WHERE NOT _fivetran_deleted --means it was deleted in source system and fivetran can't see it anymore


),

renamed AS (

  SELECT
    id::INT                 AS commodity_id,
    name::VARCHAR           AS commodity_name,
    parent_id::INT          AS commodity_parent_id,
    NULLIF(custom_fields['default-gl-acct'],'')::INT AS commodity_account_code,
    active::BOOLEAN        AS is_commodity_active

  FROM source

)

SELECT *
FROM renamed