WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'users') }}

),

renamed AS (

    SELECT  
        id                                              AS user_id,
        organization_id                                 AS organization_id,
        name                                            AS name, 
        email                                           AS email,
        phone                                           AS phone,
        restricted_agent                                AS is_restricted_agent,
        role                                            AS role,
        suspended                                       AS is_suspended,

        --time
        time_zone,
        user_fields__user_region::VARCHAR               AS user_region,
        CASE WHEN tags = '[]'
                THEN NULL
            ELSE tags
                END                                     AS tags,
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed
