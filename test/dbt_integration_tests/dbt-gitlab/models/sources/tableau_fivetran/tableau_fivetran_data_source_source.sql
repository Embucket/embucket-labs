WITH source AS (

    SELECT *
    FROM {{ source('tableau_fivetran', 'data_source') }}

), renamed AS (

    SELECT
        id::VARCHAR                        AS data_source_id,
        owner_id::VARCHAR                  AS owner_id,
        project_id::VARCHAR                AS project_id,
        has_extract::BOOLEAN               AS has_extract,
        is_certified::BOOLEAN              AS is_certified,
        created_at::TIMESTAMP              AS created_at,
        description::VARCHAR               AS data_source_description,
        use_remote_query_agent::BOOLEAN    AS use_remote_query,
        updated_at::TIMESTAMP              AS data_source_updated_at,
        name::VARCHAR                      AS data_source_name,
        content_url::VARCHAR               AS data_source_content_url,
        webpage_url::VARCHAR               AS webpage_url

    FROM source
    WHERE NOT _fivetran_deleted

)

SELECT *
FROM renamed