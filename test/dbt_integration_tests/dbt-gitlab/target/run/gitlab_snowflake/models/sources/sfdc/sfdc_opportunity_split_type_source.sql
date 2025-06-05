
  
    

create or replace transient table EMBUCKET.legacy.sfdc_opportunity_split_type_source
    

    
    as (

WITH source AS (

    SELECT
      *
    FROM EMBUCKET.salesforce_v2_stitch.opportunitysplittype

), renamed AS (

      SELECT
        
        id::VARCHAR                                                AS opportunity_split_type_id,
        createdbyid::VARCHAR                                       AS created_by_id,
        createddate::TIMESTAMP                                     AS created_date,
        lastmodifiedbyid::VARCHAR                                  AS last_modified_by_id,
        lastmodifieddate::TIMESTAMP                                AS last_modified_date,
        developername::VARCHAR                                     AS developer_name,
        description::VARCHAR                                       AS description,
        language::VARCHAR                                          AS language,
        masterlabel::VARCHAR                                       AS master_label,
        splitdatastatus::VARCHAR                                   AS split_data_status,
        splitentity::VARCHAR                                       AS split_entity,
        splitfield::VARCHAR                                        AS split_field,
        isactive::BOOLEAN                                          AS is_active,
        istotalvalidated::BOOLEAN                                  AS is_total_validated,
        isdeleted::BOOLEAN                                         AS is_deleted,
        systemmodstamp::TIMESTAMP                                  AS system_mod_timestamp

      FROM source
  )

SELECT *
FROM renamed
    )
;


  