WITH source AS (

    SELECT *
    FROM {{ source('tableau_fivetran', 'project') }}

), renamed AS (

    SELECT
        id::VARCHAR                                       AS project_id,
        controlling_permissions_project_id::VARCHAR       AS controlling_permissions_project_id,
        owner_id::VARCHAR                                 AS owner_id,
        parent_project_id::VARCHAR                        AS parent_project_id,
        created_at::TIMESTAMP                             AS created_at,
        description::VARCHAR                              AS project_description,
        content_permission::VARCHAR                       AS content_permission,
        updated_at::TIMESTAMP                             AS updated_at,
        name::VARCHAR                                     AS project_name

    FROM source
    WHERE NOT _fivetran_deleted

)

SELECT *
FROM renamed
