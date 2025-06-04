WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'departments') }}

), renamed as (

    SELECT

                --keys
                id::NUMBER                  AS department_id,
                organization_id::NUMBER     AS organization_id,
                parent_id::NUMBER           AS parent_id,

                --info
                name::VARCHAR(100)          AS department_name,
                created_at::timestamp       AS department_created_at,
                updated_at::timestamp       AS department_updated_at


    FROM source

)

SELECT 
  department_id,
  organization_id,
  parent_id,
  replace(replace(department_name,')',''),'(','')::VARCHAR(100) AS department_name,
  department_created_at,
  department_updated_at
FROM renamed
