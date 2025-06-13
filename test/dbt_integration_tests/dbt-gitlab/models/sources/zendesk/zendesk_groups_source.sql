WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'groups') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS group_id,

        --field
        url                                                 AS group_url,
        name                                                AS group_name,
        deleted                                             AS is_deleted,

        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
