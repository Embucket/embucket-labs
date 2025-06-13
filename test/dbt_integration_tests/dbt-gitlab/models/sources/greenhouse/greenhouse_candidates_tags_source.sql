WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'candidates_tags') }}

), renamed as (

    SELECT
			--keys
			  id::NUMBER 				    AS candidate_tag_id,
    		tag_id::NUMBER			   AS tag_id,
    		candidate_id::NUMBER	 AS candidate_id,

    		--info
    		created_at::timestamp  AS candidate_tag_created_at,
    		updated_at::timestamp  AS candidate_tag_updated_at


    FROM source

)

SELECT *
FROM renamed
