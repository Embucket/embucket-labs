WITH source AS (

    SELECT *
    FROM EMBUCKET.tap_zendesk.group_memberships

),

renamed AS (

    SELECT

        --ids
        id                                                  AS group_membership_id,
        group_id                                            AS group_id,
        user_id                                             AS user_id,

        --field
        "DEFAULT"                                           AS is_default_group_membership,
        url                                                 AS group_membership_url,

        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed