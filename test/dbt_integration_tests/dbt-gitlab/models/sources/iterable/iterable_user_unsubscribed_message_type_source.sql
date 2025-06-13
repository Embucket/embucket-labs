WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_unsubscribed_message_type') }}
 
), final AS (
 
    SELECT   
      message_type_id::NUMBER AS iterable_unsubscribed_message_type_id,
      _fivetran_id::VARCHAR   AS iterable_user_fivetran_id
    FROM source
)

SELECT *
FROM final
