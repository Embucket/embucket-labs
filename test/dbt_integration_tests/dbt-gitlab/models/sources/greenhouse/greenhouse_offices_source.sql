WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'offices') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER                          AS office_id,
            organization_id::NUMBER             AS organization_id,
            parent_id::NUMBER                   AS office_parent_id,

            --info
            name::varchar                       AS office_name,
            created_at::timestamp               AS office_created_at,
            updated_at::timestamp               AS office_updated_at

	FROM source

)

SELECT *
FROM renamed
