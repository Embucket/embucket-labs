
  
    

create or replace transient table EMBUCKET.sensitive.qualtrics_mailing_contacts
    

    
    as (

WITH source AS (

    SELECT *
    FROM EMBUCKET.qualtrics.contact

), intermediate AS (

    SELECT d.value as data_by_row, uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), parsed AS (

    SELECT 
      data_by_row['contactId']::VARCHAR     AS contact_id,
      data_by_row['email']::VARCHAR         AS contact_email,
      data_by_row['phone']::VARCHAR         AS contact_phone,
      data_by_row['firstName']::VARCHAR     AS contact_first_name,
      data_by_row['lastName']::VARCHAR      AS contact_last_name,
      data_by_row['mailingListId']::VARCHAR AS mailing_list_id,
      data_by_row['unsubscribed']::BOOLEAN  AS is_unsubscribed,
      uploaded_at::TIMESTAMP                AS mailing_list_membership_observed_at
    FROM intermediate

)
SELECT * 
FROM parsed
    )
;


  