WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'user_candidate_links') }}

), renamed as (

	SELECT
            --keys
            user_id::NUMBER             AS user_id,
            candidate_id::NUMBER        AS candidate_id,

            --info
            created_at::timestamp       AS user_candidate_link_created_at,
            updated_at::timestamp       AS user_candidate_link_updated_at


	FROM source

)

SELECT *
FROM renamed
