WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'deleted_mrs') }}

), renamed AS (

    SELECT 
      deleted_merge_request_id::INTEGER AS deleted_merge_request_id
    FROM source

)

SELECT * 
FROM renamed
