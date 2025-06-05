
  
    

create or replace transient table EMBUCKET.greenhouse.greenhouse_user_actions_source
    

    
    as (WITH source as (

	SELECT *
  	  FROM EMBUCKET.greenhouse.user_actions

), renamed as (

	SELECT

                --keys
                id::NUMBER          AS user_action_id,
                job_id::NUMBER      AS job_id,
                user_id::NUMBER     AS user_id,

                --info
                type::varchar       AS user_action_type


	FROM source

)

SELECT *
FROM renamed
    )
;


  