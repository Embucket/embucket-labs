
  
    

create or replace transient table EMBUCKET.greenhouse.greenhouse_interviewer_tags_source
    

    
    as (WITH source as (

	SELECT *
  	  FROM EMBUCKET.greenhouse.interviewer_tags

), renamed as (

	SELECT

        --key
        user_id::NUMBER         AS user_id,

        --info
        tag::varchar            AS interviewer_tag,
        created_at::timestamp   AS interviewer_tag_created_at,
        updated_at::timestamp   AS interviewer_tag_upated_at

	FROM source

)

SELECT *
FROM renamed
    )
;


  