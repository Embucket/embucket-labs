 WITH source AS(
 SELECT *
  FROM {{ source('coupa', 'order_header') }}
  WHERE NOT _fivetran_deleted --means it was deleted in source system and fivetran can't see it anymore


),

renamed AS (

  SELECT
    id::INT                AS po_id,
    po_number::INT         AS po_number,
    supplier_id::INT       AS supplier_id,
    status::VARCHAR        AS po_status,
    currency_id::INT       AS currency_id,
    payment_term_id::INT   AS payment_term_id,
    requisition_header_id::INT AS requisition_id,
    
    
  FROM source

)

SELECT *
FROM renamed
