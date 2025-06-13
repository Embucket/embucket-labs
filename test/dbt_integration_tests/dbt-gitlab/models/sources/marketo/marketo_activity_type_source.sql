WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_type') }}

), renamed AS (

    SELECT

      id::NUMBER                AS marketo_activity_type_id,
      name::TEXT                AS name,
      description::TEXT         AS description

    FROM source

)

SELECT *
FROM renamed
