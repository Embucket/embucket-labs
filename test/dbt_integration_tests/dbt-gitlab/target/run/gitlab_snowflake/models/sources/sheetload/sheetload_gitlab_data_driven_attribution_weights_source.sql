
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_gitlab_data_driven_attribution_weights_source
    

    
    as (WITH source AS (

        SELECT
                channel::TEXT AS channel,
                offer_type::TEXT AS offer_type,
                weight::FLOAT AS weight
        FROM EMBUCKET.sheetload.gitlab_data_driven_attribution_weights

        )
        SELECT * 
        FROM source
    )
;


  