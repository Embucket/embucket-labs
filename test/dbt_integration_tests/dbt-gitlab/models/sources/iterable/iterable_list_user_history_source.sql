WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','list_user_history') }}
 
), final AS (

    SELECT
        list_id::VARCHAR          AS iterable_list_id,
        updated_at::TIMESTAMP     AS iterable_list_user_history_updated_at,
        updated_at::DATE          AS iterable_list_user_history_updated_date,
        _fivetran_id::VARCHAR     AS iterable_list_user_fivetran_id
    FROM source

)

SELECT *
FROM final