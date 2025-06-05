
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_marketo_lead_scores_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.marketo_lead_scores

), renamed AS (

    SELECT
        activity_type::VARCHAR AS activity_type,
        scored_action::VARCHAR AS scored_action,
        current_score::INT     AS current_score,
        previous_score::INT    AS previous_score,
        test_score::INT        AS test_score
    FROM source

)

SELECT *
FROM renamed
    )
;


  