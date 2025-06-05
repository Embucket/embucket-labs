
  
    

create or replace transient table EMBUCKET.sfdc.sfdc_record_type_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.salesforce_v2_stitch.recordtype


), renamed AS (

    SELECT
         id                AS record_type_id,
         developername     AS record_type_name,
        --keys
         businessprocessid AS business_process_id,
        --info
         name              AS record_type_label,
         description       AS record_type_description,
         sobjecttype       AS record_type_modifying_object_type

    FROM source

)

SELECT *
FROM renamed
    )
;


  