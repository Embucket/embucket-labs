WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','channel') }}
 
), final AS (
 
    SELECT
        id::VARCHAR                AS iterable_channel_id,
        channel_type::VARCHAR      AS iterable_channel_type,
        message_medium::VARCHAR    AS iterable_channel_message_medium,
        name::VARCHAR              AS iterable_channel_name
    FROM source

)

SELECT *
FROM final
