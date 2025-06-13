WITH source AS (

    SELECT *
    FROM {{ ref('iterable_user_unsubscribed_channel_source') }}

)

SELECT *
FROM source