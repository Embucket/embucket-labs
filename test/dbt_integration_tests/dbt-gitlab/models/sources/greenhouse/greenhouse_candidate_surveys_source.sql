WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'candidate_surveys') }}

), renamed as (

    SELECT

    		--keys
    		id::NUMBER 					      AS candidate_survey_id,
    		organization_id::NUMBER	  AS organization_id,
    		department_id::NUMBER     AS department_id,
    		office_id::NUMBER			    AS office_id,

   			--info
    		department_name::varchar	AS department_name,
    		office_name::varchar		  AS office_name,
    		question_1::varchar			  AS candidate_survey_question_1,
    		question_2::varchar			  AS candidate_survey_question_2,
    		question_3::varchar			  AS candidate_survey_question_3,
    		question_4::varchar			  AS candidate_survey_question_4,
    		question_5::varchar			  AS candidate_survey_question_5,
    		question_6::varchar			  AS candidate_survey_question_6,
    		question_7::varchar			  AS candidate_survey_question_7,
    		question_8::varchar			  AS candidate_survey_question_8,
     		submitted_at::timestamp 	AS candidate_survey_submitted_at

    FROM source

)

SELECT *
FROM renamed
