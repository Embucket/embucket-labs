WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'macros') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS macro_id,

        --field
        active                                              AS is_active,
        url                                                 AS macro_url,
        description                                         AS macro_description,
        position                                            AS macro_position,
        title                                               AS macro_title,

        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
