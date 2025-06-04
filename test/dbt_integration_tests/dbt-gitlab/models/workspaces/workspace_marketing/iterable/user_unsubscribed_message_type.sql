WITH source AS (

    SELECT *
    FROM {{ ref('iterable_user_unsubscribed_message_type_source') }}

)

SELECT *
FROM source