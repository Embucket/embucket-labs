
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_award_emoji_dedupe_source
    

    
    as (WITH award_emoji AS (

    SELECT *
    FROM EMBUCKET.tap_postgres.gitlab_db_award_emoji
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

)

SELECT *
FROM award_emoji
    )
;


  