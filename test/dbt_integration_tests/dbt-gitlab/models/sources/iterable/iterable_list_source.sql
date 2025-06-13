WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','list') }}
 
), final AS (

    SELECT
        id::VARCHAR              AS iterable_list_id,
        created_at::TIMESTAMP    AS iterable_list_created_at,
        created_at::DATE         AS iterable_list_created_date,
        description::VARCHAR     AS iterable_list_description,
        list_type::VARCHAR       AS iterable_list_type,
        name::VARCHAR            AS iterable_list_name
    FROM source

)

SELECT *
FROM final