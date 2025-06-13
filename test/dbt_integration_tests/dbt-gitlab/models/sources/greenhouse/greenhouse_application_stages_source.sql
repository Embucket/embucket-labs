WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'application_stages') }}

), stage_dim AS (

    SELECT *
    FROM  {{ ref ('greenhouse_stages_source') }}

), renamed as (

	SELECT
    		--keys
    		application_id::NUMBER		    AS application_id,
    		stage_id::NUMBER			    AS stage_id,

    		--info
    		entered_on::timestamp 		    AS stage_entered_on,
    		exited_on::timestamp 		    AS stage_exited_on,
    		stage_name::varchar 		    AS application_stage_name

	FROM source

), intermediate AS (

    SELECT 
      renamed.*,
      stage_dim.is_milestone_stage,
      IFF(renamed.stage_id IS NULL AND renamed.application_stage_name = 'Hired', -1, renamed.stage_id) AS modified_stage_id,
      stage_dim.stage_name_modified,
      IFF(stage_dim.stage_name_modified = 'Team Interview - Face to Face',
            'team_interview',
            LOWER(REPLACE(stage_dim.stage_name_modified, ' ', '_'))) AS stage_name_modified_with_underscores
    FROM renamed
    LEFT JOIN stage_dim 
      ON renamed.stage_id = stage_dim.stage_id

)

SELECT *
FROM intermediate
