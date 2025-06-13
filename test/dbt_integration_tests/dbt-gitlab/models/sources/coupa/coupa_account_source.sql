 WITH source AS(
 SELECT *
  FROM {{ source('coupa', 'account') }}
  WHERE NOT _fivetran_deleted --means it was deleted in source system and fivetran can't see it anymore


),

renamed AS (

  SELECT
    id::INT                 AS account_id,
    account_type_id::INT    AS account_type_id,
    code::VARCHAR           AS account_code,
    SPLIT_PART(name,'-',2)::VARCHAR     AS account_department,
    segment_1::INT          AS account_entity_code,
    segment_2::INT          AS account_department_code,
    segment_3::INT          AS account_commodity_code

  FROM source

)

SELECT *
FROM renamed