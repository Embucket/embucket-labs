WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','person_name') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    personal_info_system_id::VARCHAR     AS personal_info_system_id,
    type::VARCHAR                        AS type,
    index::VARCHAR                       AS index,
    first_name::VARCHAR                  AS first_name,
    last_name::VARCHAR                   AS last_name
  FROM source

)

SELECT *
FROM final