
  
    

create or replace transient table EMBUCKET.zendesk.zendesk_tickets_dedupe_source
    

    
    as (

SELECT *
FROM EMBUCKET.tap_zendesk.tickets

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
    )
;


  