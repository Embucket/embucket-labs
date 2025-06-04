WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','message_type') }}
 
), final AS (

    SELECT
        id::VARCHAR                         AS iterable_message_type_id,
        updated_at::TIMESTAMP               AS iterable_message_updated_at,
        updated_at::DATE                    AS iterable_message_updated_date,
        created_at::TIMESTAMP               AS iterable_message_created_at,
        created_at::DATE                    AS iterable_message_created_date,
        channel_id::VARCHAR                 AS iterable_message_channel_id,
        rate_limit_per_minute::VARCHAR      AS iterable_message_rate_limit_per_minute,
        frequency_cap::VARCHAR              AS iterable_message_frequency_cap,
        subscription_policy::VARCHAR        AS iterable_message_subscription_policy,
        name::VARCHAR                       AS iterable_message_name
    FROM source

)

SELECT *
FROM final