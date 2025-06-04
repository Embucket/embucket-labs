WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_unsubscribed_channel') }}
 
), final AS (
 
    SELECT   
      channel_id::NUMBER     AS iterable_user_unsubscribed_channel_id,
      _fivetran_id::VARCHAR  AS iterable_user_unsubscribed_fivetran_id
    FROM source
)

SELECT *
FROM final
