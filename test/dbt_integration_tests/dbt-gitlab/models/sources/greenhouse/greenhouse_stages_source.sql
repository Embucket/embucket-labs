WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'stages') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER                  AS stage_id,
            organization_id::NUMBER     AS organization_id,

            --info
            name::varchar             	AS stage_name,
            "order"::NUMBER                AS stage_order,
            active::boolean             AS is_active


	FROM source

), final AS (

    SELECT *,
      CASE WHEN LOWER(stage_name) LIKE '%screen%'
             THEN 'Screen'
           WHEN LOWER(stage_name) LIKE '%executive interview%'
             THEN 'Executive Interview'  
           WHEN LOWER(stage_name) LIKE '%interview%'
             THEN 'Team Interview - Face to Face'
           WHEN LOWER(stage_name) LIKE '%assessment%'
             THEN 'Take Home Assessment'
           WHEN LOWER(stage_name) LIKE '%take home%'
             THEN 'Take Home Assessment'
           WHEN stage_name IN ('Hiring Manager Review','Preliminary Phone Screen')
             THEN 'Hiring Manager Review'
           WHEN LOWER(stage_name) LIKE '%reference%'
             THEN 'Reference Check'
           WHEN LOWER(stage_name) LIKE '%background check & offer'
             THEN 'Offer'           
           ELSE stage_name END::VARCHAR(100)                           AS stage_name_modified,
      IFF(stage_name_modified IN ('Application Review' 
                                 ,'Screen'
                                 ,'Hiring Manager Review'
                                 ,'Take Home Assessment'
                                 ,'Executive Interview'
                                 ,'Reference Check'
                                 ,'Offer'), TRUE, FALSE)               AS is_milestone_stage
    FROM renamed

)

SELECT *
FROM final
