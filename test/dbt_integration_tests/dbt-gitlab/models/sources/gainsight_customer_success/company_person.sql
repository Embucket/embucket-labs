{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','company_person') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                      AS gsid,
    _fivetran_deleted::BOOLEAN         AS _fivetran_deleted,
    survey_name_new_gc::VARCHAR        AS survey_name_new_gc,
        email_opt_out_gc::BOOLEAN      AS email_opt_out_gc,
    delete_gc::VARCHAR                 AS delete_gc,
    git_lab_role_gc::VARCHAR           AS git_lab_role_gc,
    modified_date::TIMESTAMP           AS modified_date,
    modified_by::VARCHAR               AS modified_by,
    manager::VARCHAR                   AS manager,
    gs_ingestion_source::VARCHAR       AS gs_ingestion_source,
    sfdc_link_gc::VARCHAR              AS sfdc_link_gc,
    survey_response_id_gc::VARCHAR     AS survey_response_id_gc,
    sfdc_account_id::VARCHAR           AS sfdc_account_id,
    created_by::VARCHAR                AS created_by,
    title::VARCHAR                     AS title,
    contact_owner_gc::VARCHAR          AS contact_owner_gc,
    person_id::VARCHAR                 AS person_id,
    inactive_contact_gc::VARCHAR       AS inactive_contact_gc,
    role::VARCHAR                      AS role,
    what_features_question_gc::VARCHAR AS what_features_question_gc,
    active::VARCHAR                    AS active,
    company_id::VARCHAR                AS company_id,
    created_date::TIMESTAMP            AS created_date,
    sfdc_contact_id::VARCHAR           AS sfdc_contact_id,
    mobile_phone_gc::VARCHAR           AS mobile_phone_gc,
    _fivetran_synced::TIMESTAMP        AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed
