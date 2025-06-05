
  
    

create or replace transient table EMBUCKET.driveload.driveload_email_domain_classification_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.driveload.email_domain_classification

)

SELECT
  domain::VARCHAR               AS domain,
  classification::VARCHAR       AS classification
FROM source
    )
;


  