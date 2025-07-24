WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'dup_tap_zendesk_tags') }}

),

renamed AS (

    SELECT

        count AS tag_count,
        name  AS tag_name

    FROM source

)

SELECT *
FROM renamed
